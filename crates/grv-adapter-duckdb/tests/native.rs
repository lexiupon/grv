#![cfg(feature = "native")]
use grv_adapter_duckdb::{
    conversion::{Scalar, SourceType},
    worker::Engine,
};
use grv_adapter_duckdb::{lock::WorkspaceLock, native::NativeEngine, worker::Interrupt};
use std::{
    thread,
    time::{Duration, Instant},
};

#[test]
fn pinned_native_worker_streams_only_demanded_bounded_chunks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("native.duckdb");
    let mut worker = NativeEngine::spawn(&path).unwrap();
    assert!(WorkspaceLock::acquire(&path).is_err());
    worker.begin("SELECT * FROM range(5000)").unwrap();
    let mut rows = 0;
    while let Some(bytes) = worker.fetch(8 * 1024 * 1024).unwrap() {
        assert!(bytes.len() <= 8 * 1024 * 1024);
        rows += u64::from_le_bytes(bytes[..8].try_into().unwrap());
    }
    assert_eq!(rows, 5000);
    assert!(
        worker
            .begin("SELECT * FROM read_csv('/tmp/should-not-read')")
            .is_err()
    );
    assert!(worker.begin("CREATE TABLE unwanted(i BIGINT)").is_err());
    let retained_interrupt = worker.interrupt_handle();
    worker.stop().unwrap();
    retained_interrupt.interrupt(); // Inert, safe after native connections close.
    assert!(WorkspaceLock::acquire(&path).is_ok());
}

#[test]
fn interrupt_remains_safe_during_query_and_after_owner_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("interrupt.duckdb");
    let mut worker = NativeEngine::spawn(&path).unwrap();
    let interrupt = worker.interrupt_handle();
    let trigger = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        // Repeating handles the tiny race between prepare and query start.
        for _ in 0..20 {
            interrupt.interrupt();
            thread::sleep(Duration::from_millis(10));
        }
        interrupt
    });
    let started = Instant::now();
    let outcome = worker.begin("SELECT sum(range)::BIGINT FROM range(100000000000)");
    assert!(outcome.is_err());
    assert!(started.elapsed() < Duration::from_secs(5));
    let interrupt = trigger.join().unwrap();
    worker.stop().unwrap();
    interrupt.interrupt();
    assert!(WorkspaceLock::acquire(&path).is_ok());
}

#[test]
fn staged_acquisition_supports_exact_native_types_and_refuses_reacquisition() {
    let directory = tempfile::tempdir().unwrap();
    let mut engine = NativeEngine::open(&directory.path().join("typed.duckdb")).unwrap();
    let schema = engine.acquire("SELECT true, -127::TINYINT, -32767::SMALLINT, -2147483647::INTEGER, -9223372036854775807::BIGINT, -0.0::FLOAT, 1.25::DOUBLE, '🍕'::VARCHAR, '\\x00\\xFF'::BLOB, DATE '1970-01-02', 1234567890123456789012345678.12::DECIMAL(30,2), TIMESTAMP_S '1970-01-01 00:00:01', TIMESTAMP_MS '1970-01-01 00:00:00.001', TIMESTAMP '1970-01-01 00:00:00.000001', TIMESTAMP_NS '1970-01-01 00:00:00.000000001', TIMESTAMPTZ '1970-01-01 00:00:00+00'").unwrap();
    assert_eq!(schema.rows, 1);
    assert_eq!(schema.types.len(), 16);
    assert_eq!(
        schema.types[10],
        SourceType::Decimal128 {
            precision: 30,
            scale: 2
        }
    );
    assert!(engine.acquire("SELECT 42::BIGINT").is_err());
    assert!(
        engine
            .begin("SELECT * FROM temp.main.\"_grv_acquisition_0\"")
            .is_err()
    );
    assert!(
        engine
            .authorize_relation("main", "_grv_acquisition_0")
            .is_err()
    );
    let window = engine
        .fetch_acquired(&schema, 4096, 32 * 1024 * 1024)
        .unwrap()
        .unwrap();
    assert_eq!(window.row_count(), 1);
    window
        .visit(|_, column, value| {
            match column {
                0 => assert_eq!(value, Scalar::Boolean(true)),
                1 => assert_eq!(value, Scalar::Integer(-127)),
                7 => assert_eq!(value, Scalar::Utf8("🍕".as_bytes())),
                8 => assert_eq!(value, Scalar::Binary(&[0, 255])),
                9 => assert_eq!(value, Scalar::Date32(1)),
                10 => assert_eq!(value, Scalar::Decimal128(123456789012345678901234567812)),
                11..=14 => assert_eq!(value, Scalar::Timestamp(1)),
                15 => assert_eq!(value, Scalar::Timestamp(0)),
                _ => {}
            }
            Ok(())
        })
        .unwrap();
    let fields = schema
        .types
        .iter()
        .enumerate()
        .map(|(index, source)| {
            use arrow_schema::{DataType, Field, TimeUnit};
            use grv_adapter_duckdb::conversion::TickUnit;
            let output = match source {
                SourceType::Boolean => DataType::Boolean,
                SourceType::SignedInteger { .. } => DataType::Int64,
                SourceType::Float32 | SourceType::Float64 => DataType::Float64,
                SourceType::Utf8 => DataType::Utf8,
                SourceType::Binary => DataType::Binary,
                SourceType::Date32 => DataType::Date32,
                SourceType::Decimal128 { .. } => DataType::Decimal128(38, 10),
                SourceType::Timestamp { unit, utc } => DataType::Timestamp(
                    match unit {
                        TickUnit::Nanosecond => TimeUnit::Nanosecond,
                        _ => TimeUnit::Microsecond,
                    },
                    if *utc { Some("UTC".into()) } else { None },
                ),
                _ => panic!("unsupported native test type"),
            };
            Field::new(format!("column{index}"), output, true)
        })
        .collect::<Vec<_>>();
    let output_schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
    let ipc = grv_adapter_duckdb::ipc::encode(
        &window,
        output_schema.clone(),
        8 * 1024 * 1024,
        32 * 1024 * 1024,
    )
    .unwrap();
    // The protocol forbids a trailing end-of-stream message. Walk the actual
    // flatbuffer message boundaries, rather than guessing from trailing zeros.
    let mut position = 0;
    let mut messages = 0;
    while position < ipc.len() {
        assert_eq!(&ipc[position..position + 4], &[255; 4]);
        let metadata =
            u32::from_le_bytes(ipc[position + 4..position + 8].try_into().unwrap()) as usize;
        assert_ne!(metadata, 0, "unexpected EOS message");
        let message =
            arrow_ipc::root_as_message(&ipc[position + 8..position + 8 + metadata]).unwrap();
        position += 8 + metadata + message.bodyLength() as usize;
        messages += 1;
    }
    assert_eq!(position, ipc.len());
    assert_eq!(messages, 2);
    let mut reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(&ipc), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.schema(), output_schema);
    assert_eq!(batch.num_rows(), 1);
    assert!(reader.next().is_none());
    assert!(
        grv_adapter_duckdb::ipc::encode(&window, output_schema.clone(), 128, 32 * 1024 * 1024)
            .is_err()
    );
    assert!(
        grv_adapter_duckdb::ipc::encode(&window, output_schema, 8 * 1024 * 1024, 1024).is_err()
    );
    assert!(
        engine
            .fetch_acquired(&schema, 4096, 32 * 1024 * 1024)
            .unwrap()
            .is_none()
    );
}

#[test]
fn staged_windows_split_variable_values_before_materialization_and_handle_empty_sources() {
    let directory = tempfile::tempdir().unwrap();
    let mut engine = NativeEngine::open(&directory.path().join("windows.duckdb")).unwrap();
    let schema = engine
        .acquire("SELECT 'abcdefghijklmnop', range FROM range(5000)")
        .unwrap();
    let mut rows = 0;
    while let Some(window) = engine.fetch_acquired(&schema, 1024, 256 * 1024).unwrap() {
        assert!(window.encoded_bytes() <= 1024);
        rows += window.row_count();
    }
    assert_eq!(rows, 5000);
    drop(engine);
    let mut engine = NativeEngine::open(&directory.path().join("oversize.duckdb")).unwrap();
    let schema = engine
        .acquire(&format!("SELECT '{}'", "x".repeat(4096)))
        .unwrap();
    assert!(
        engine
            .fetch_acquired(&schema, 1024, 32 * 1024 * 1024)
            .is_err()
    );
    drop(engine);
    let mut engine = NativeEngine::open(&directory.path().join("empty.duckdb")).unwrap();
    let schema = engine.acquire("SELECT range FROM range(0)").unwrap();
    assert_eq!(schema.rows, 0);
    assert!(
        engine
            .fetch_acquired(&schema, 1024, 32 * 1024 * 1024)
            .unwrap()
            .is_none()
    );
}
