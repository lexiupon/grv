//! Opt-in production CLI gates for the versioned disposable-org fixture.
//! REST/auto acquire the complete rich projection. Explicit Bulk must either
//! preserve that same contract or be tested as a precise refusal, never by
//! dropping nullable text/relationship fields. Only the separately authorized
//! mutation gate writes the pinned disposable fixture and resets it afterward.
use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use grv_adapter_host::validate_output;
use grv_core::{
    source::{self, TableSelection},
    store::Store,
};
use grv_storage::LocalBackend;
use grv_types::{Name, RequestedRevision, U64};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

const AUTHORIZED_ORG: &str = "fixture-user@example.invalid";
const AUTHORIZED_ID: &str = "00D000000000001AAA";

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../spec/fixtures/release-validation/salesforce")
}

fn cli(adapters: &Path, args: &[&str], offline: Option<&Path>) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"));
    command
        .arg("--json")
        .args(args)
        .env("GRV_ADAPTERS_DIR", adapters);
    if let Some(home) = offline {
        // Child-only environment: no credential store or executable helpers.
        command.env("HOME", home).env("PATH", home);
    }
    let output = command.output().unwrap();
    let value: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("live CLI returned no JSON; exit {:?}", output.status.code()));
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}

struct Environment {
    org: String,
    expected: String,
    transport: String,
    executable: OsString,
}

impl Environment {
    fn explicit() -> Self {
        let org = std::env::var("GRV_SALESFORCE_TEST_ORG").expect("explicit disposable test org");
        let expected =
            std::env::var("GRV_SALESFORCE_TEST_ORG_ID").expect("explicit expected org ID");
        // This authorization is for exactly this disposable org, not a default
        // alias or an arbitrary (possibly production) environment override.
        assert_eq!(org, AUTHORIZED_ORG);
        assert_eq!(expected, AUTHORIZED_ID);
        grv_adapter_salesforce::auth::OrgId::parse(&expected).unwrap();
        let transport = match std::env::var("GRV_SALESFORCE_TEST_TRANSPORT") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "auto".into(),
            Err(std::env::VarError::NotUnicode(_)) => panic!("test transport must be UTF-8"),
        };
        assert!(matches!(transport.as_str(), "auto" | "rest" | "bulk"));
        Self {
            org,
            expected,
            transport,
            executable: std::env::var_os("GRV_SALESFORCE_TEST_ADAPTER")
                .expect("production adapter executable"),
        }
    }

    fn authenticate(&self, expected: &str) -> grv_adapter_salesforce::Result<String> {
        let home = std::env::var_os("HOME").expect("offline org metadata HOME");
        let locator = grv_adapter_salesforce::auth::locate_connection(
            &grv_adapter_salesforce::config::Connection {
                org: self.org.clone(),
                api_version: "v66.0".into(),
            },
            &mut grv_adapter_salesforce::offline::FileMetadataStore::for_home(home.into()),
        )?;
        let mut bound = grv_adapter_salesforce::auth::BoundConnection::bind(locator)?;
        let mut auth = grv_adapter_salesforce::auth::CliAuthentication {
            program: "sf".into(),
            supervisor: Some(self.executable.clone()),
            verifier: grv_adapter_salesforce::http::SalesforceHttp {
                executor: grv_adapter_salesforce::http::CurlHttp {
                    supervisor: Some(self.executable.clone()),
                    ..Default::default()
                },
            },
            cancellation: Default::default(),
        };
        let result = bound.authenticate(&mut auth, Some(expected));
        if result.is_err() {
            // A failed identity guard exposes no session to any query/job path.
            assert!(bound.session().is_err());
            assert!(bound.shared_session().is_err());
        }
        result
    }
}

struct Harness {
    temp: tempfile::TempDir,
    adapters: PathBuf,
    root: PathBuf,
    state: PathBuf,
    environment: Environment,
}

impl Harness {
    fn new() -> Self {
        let environment = Environment::explicit();
        let identity = format!("salesforce:{}", environment.expected);
        // Verify the authorized org before packaging/init/capture and, in
        // particular, before any read-only Bulk query job can be created.
        assert_eq!(environment.authenticate(&identity).unwrap(), identity);
        let temp = tempfile::tempdir_in(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap(),
        )
        .unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let adapters = temp.path().join("adapters");
        let package = adapters.join("salesforce");
        fs::create_dir_all(&package).unwrap();
        fs::set_permissions(&adapters, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&package, fs::Permissions::from_mode(0o700)).unwrap();
        let binary = package.join("adapter");
        fs::copy(&environment.executable, &binary).unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(package.join("adapter.toml"), format!(
            "name = 'salesforce'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\nentrypoint_sha256 = {:?}\n",
            binary, grv_types::sha256(&fs::read(&binary).unwrap()).as_str())).unwrap();
        let root = temp.path().join("grv");
        let state = temp.path().join("state");
        assert_eq!(
            cli(&adapters, &["init", "--grv", root.to_str().unwrap()], None)["ok"],
            true
        );
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(&state).unwrap();
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            temp,
            adapters,
            root,
            state,
            environment,
        }
    }

    fn declaration(&self, selection: &str) -> PathBuf {
        let original =
            fs::read_to_string(fixture().join(format!("decl/push-{selection}.yml"))).unwrap();
        assert_eq!(original.matches("  org: grv-fixture").count(), 1);
        assert_eq!(original.matches("  transport: auto").count(), 1);
        // Consume the complete committed declaration; change only the explicit
        // connection and transport. In particular, preserve all source fields.
        let patched = original
            .replace(
                "  org: grv-fixture",
                &format!(
                    "  org: {}",
                    serde_json::to_string(&self.environment.org).unwrap()
                ),
            )
            .replace(
                "  transport: auto",
                &format!("  transport: {}", self.environment.transport),
            );
        let path = self.temp.path().join(format!("push-{selection}.yml"));
        fs::write(&path, patched).unwrap();
        path
    }

    fn push(&self, decl: &Path, attempt: &grv_types::Uuid, offline: Option<&Path>) -> Value {
        cli(
            &self.adapters,
            &[
                "push",
                "--grv",
                self.root.to_str().unwrap(),
                "--decl",
                decl.to_str().unwrap(),
                "--state",
                self.state.to_str().unwrap(),
                "--attempt",
                attempt.as_str(),
            ],
            offline,
        )
    }

    fn capture(&self, selection: &str, oracle: &Value) -> Capture {
        let decl = self.declaration(selection);
        let first_attempt = grv_types::Uuid::v4();
        let first = self.push(&decl, &first_attempt, None);
        assert_eq!(first["ok"], true, "{selection}: {first}");
        if selection == "empty" {
            // A first empty partitioned snapshot has no partition/version to
            // publish. Its accepted capture still retains the full contract.
            assert_eq!(first["result"]["outcome"]["kind"], "no-op");
            assert_eq!(first["result"]["outcome"]["revision"], "0");
            assert!(first["result"]["outcome"]["operation_id"].is_null());
        } else {
            assert_eq!(first["result"]["outcome"]["kind"], "published");
            assert_eq!(first["result"]["outcome"]["revision"], "1");
            assert!(first["result"]["outcome"]["operation_id"].is_string());
        }
        assert_eq!(first["result"]["replayed"], false);
        let identities = if selection == "empty" {
            verify_empty_capture(self, &first_attempt);
            Identities::default()
        } else {
            verify_rows(self, selection, oracle)
        };
        let second_attempt = grv_types::Uuid::v4();
        let second = self.push(&decl, &second_attempt, None);
        assert_eq!(second["ok"], true, "{selection}: {second}");
        assert_eq!(second["result"]["replayed"], false);
        assert_eq!(second["result"]["outcome"]["kind"], "no-op");
        assert_eq!(
            second["result"]["outcome"]["revision"],
            if selection == "empty" { "0" } else { "1" }
        );
        assert!(second["result"]["outcome"]["operation_id"].is_null());
        assert_ne!(first["result"]["run_id"], second["result"]["run_id"]);
        if selection == "empty" {
            verify_empty_capture(self, &second_attempt);
        } else {
            assert_eq!(verify_rows(self, selection, oracle), identities);
        }
        Capture {
            decl,
            first_attempt,
            first,
            second_attempt,
            second,
            identities,
        }
    }

    fn terminal_replays(&self, captures: &[Capture]) {
        fs::remove_dir_all(self.adapters.join("salesforce")).unwrap();
        fs::remove_dir_all(&self.root).unwrap();
        let offline = self.temp.path().join("no-source-home-or-helpers");
        fs::create_dir(&offline).unwrap();
        for capture in captures {
            for (attempt, original) in [
                (&capture.first_attempt, &capture.first),
                (&capture.second_attempt, &capture.second),
            ] {
                let replay = self.push(&capture.decl, attempt, Some(&offline));
                assert_eq!(replay["ok"], true, "{replay}");
                assert_eq!(replay["result"]["replayed"], true);
                let mut expected = original.clone();
                expected["result"]["replayed"] = json!(true);
                assert_eq!(replay["result"], expected["result"]);
            }
        }
        assert!(!self.root.exists(), "terminal replay recreated GRV");
    }

    fn bulk_refusal(&self, selection: &str) {
        assert_eq!(self.environment.transport, "bulk");
        let decl = self.declaration(selection);
        let result = self.push(&decl, &grv_types::Uuid::v4(), None);
        assert_eq!(result["ok"], false, "{result}");
        assert_eq!(
            result["errors"][0]["code"], "UNSUPPORTED_CAPABILITY",
            "{result}"
        );
        let message = result["errors"][0]["message"].as_str().unwrap();
        // The provider can mark even Name nillable. Refusal must identify
        // an actual text projection; do not assume which is encountered first.
        let columns: &[&str] = if selection == "rel" {
            &["name", "status", "rel_name", "rel_text"]
        } else {
            &["name", "text", "long_text", "picklist", "formula", "status"]
        };
        assert!(
            columns
                .iter()
                .any(|column| message.contains(&format!("column {column}:"))),
            "{result}"
        );
        assert!(message.contains("Bulk CSV cannot distinguish null and empty values for this source projection; select REST"), "{result}");
        let dataset = dataset(selection);
        // Ownership setup may initialize revision-zero coordination before
        // Describe refuses the projection. This is not a published revision.
        let latest_path = self.root.join(format!("datasets/{dataset}/.states/LATEST"));
        if latest_path.exists() {
            let latest: grv_storage::model::Latest =
                grv_storage::model::decode_record(&fs::read(latest_path).unwrap()).unwrap();
            assert_eq!(latest.revision.get(), 0);
            assert_eq!(latest.high_water.get(), 0);
            assert!(latest.pending.is_none());
        }
        assert!(
            !self
                .root
                .join(format!("datasets/{dataset}/grvfix/version=1"))
                .exists()
        );
        eprintln!(
            "{selection}: precise complete-contract Bulk refusal, NOT rich Bulk acquisition coverage"
        );
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Identities {
    main: BTreeMap<usize, String>,
    related: BTreeMap<usize, String>,
}

struct Capture {
    decl: PathBuf,
    first_attempt: grv_types::Uuid,
    first: Value,
    second_attempt: grv_types::Uuid,
    second: Value,
    identities: Identities,
}

fn dataset(selection: &str) -> &str {
    match selection {
        "base" => "grv_sf_fixture",
        "empty" => "grv_sf_fixture_empty",
        "rel" => "grv_sf_fixture_rel",
        _ => panic!("unknown selection"),
    }
}

// Import only the independent Python stdlib generator. It writes nothing and
// emits exact Arrow-domain oracle values for EVERY selected row. Decimal uses
// integer coefficients, date/timestamp use integer epoch offsets (no floats).
fn oracle() -> Value {
    let script = r#"
import importlib.util, json, sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
p = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location('fixture_oracle', p / 'generate.py')
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)
manifest = json.loads((p / 'manifest/value-manifest.json').read_text())
assert manifest == json.loads(json.dumps(g.build_manifest()[0])), 'committed manifest differs from independent generator'
assert manifest['manifest_version'] == 3
assert manifest['selections']['base']['row_count'] == 900
assert manifest['selections']['empty']['row_count'] == 0
assert manifest['selections']['empty']['filter'] == "GrvStatus__c = 'Active' AND Name = 'GRVFIX-EMPTY-NONEXISTENT'"
assert g.grvfix_stored_values(5)['GrvDecimal__c'] == '99999999.1250000000'

def convert(row):
    row = dict(row)
    for key, value in row.items():
        if value is None:
            continue
        if key in ('decimal_value', 'rel_decimal'):
            scale = 10 if key == 'decimal_value' else 6
            coefficient = Decimal(value) * (10 ** scale)
            assert coefficient == coefficient.to_integral_value(), (key, value)
            row[key] = str(int(coefficient))
        elif key in ('date_value', 'partition_date', 'rel_date'):
            row[key] = (date.fromisoformat(value) - date(1970, 1, 1)).days
        elif key == 'datetime_value':
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            assert dt.microsecond == 0, value
            delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
            row[key] = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
    return row

base, rel = {}, {}
for i in range(g.ROWS):
    stored = g.grvfix_stored_values(i)
    if stored['GrvStatus__c'] != 'Active':
        assert i % 10 == 7
        continue
    row = g.grvfix_grv_values(i)
    del row['id']
    del row['rel_id']
    base[str(i)] = convert(row)
    related = g.grvfixrel_values(i) if i < g.REL_ROWS else {}
    rr = {key: row[key] for key in ('name', 'status', 'partition_date', '_month_')}
    rr.update({target: related.get(source) for target, source in (
        ('rel_name', 'Name'), ('rel_text', 'GrvRelText__c'),
        ('rel_decimal', 'GrvRelDecimal__c'), ('rel_date', 'GrvRelDate__c'))})
    rel[str(i)] = convert(rr)
assert len(base) == len(rel) == 900
print(json.dumps({'manifest': manifest, 'base': base, 'rel': rel, 'empty': {}}, ensure_ascii=False))
"#;
    let output = Command::new("python3")
        .arg("-B")
        .arg("-c")
        .arg(script)
        .arg(fixture())
        .output()
        .expect("python3 stdlib independent fixture oracle");
    assert!(
        output.status.success(),
        "oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn column_type(name: &str) -> DataType {
    match name {
        "int_value" => DataType::Int64,
        "bool_value" => DataType::Boolean,
        "date_value" | "partition_date" | "rel_date" => DataType::Date32,
        "datetime_value" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "decimal_value" => DataType::Decimal128(38, 10),
        "rel_decimal" => DataType::Decimal128(38, 6),
        "id" | "name" | "text" | "long_text" | "picklist" | "formula" | "status" | "rel_id"
        | "rel_name" | "rel_text" | "_month_" => DataType::Utf8,
        other => panic!("unexpected column {other}"),
    }
}

fn scalar(batch: &RecordBatch, column: usize, row: usize) -> Value {
    let array = batch.column(column);
    // Validate exact types even for all-null batches; never cast or round.
    let value = match array.data_type() {
        DataType::Utf8 => json!(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
        ),
        DataType::Int64 => json!(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
        ),
        DataType::Boolean => json!(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row)
        ),
        DataType::Date32 => json!(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row)
        ),
        DataType::Decimal128(38, scale) => {
            let name = batch.schema().field(column).name().clone();
            assert_eq!(*scale, if name == "rel_decimal" { 6 } else { 10 });
            json!(
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(row)
                    .to_string()
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, zone) => {
            assert_eq!(zone.as_deref(), Some("UTC"));
            json!(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(row)
            )
        }
        other => panic!("unexpected persisted scalar type {other:?}"),
    };
    if array.is_null(row) {
        Value::Null
    } else {
        value
    }
}

fn record_id(value: &Value) -> String {
    let id = value.as_str().expect("non-null record Id");
    assert_eq!(id.len(), 18);
    assert!(id.bytes().all(|byte| byte.is_ascii_alphanumeric()));
    id.to_owned()
}

fn verify_empty_capture(h: &Harness, attempt: &grv_types::Uuid) {
    let path = h
        .state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");
    let journal: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let capture: grv_core::capture::CaptureReceipt =
        serde_json::from_value(journal["evidence"]["capture"].clone()).unwrap();
    assert_eq!(capture.tables.len(), 1);
    let table = &capture.tables[0];
    assert_eq!(table.row_count.get(), 0);
    assert!(table.groups.is_empty());
    assert_eq!(table.plan.contract.columns.len(), 15);
    assert_eq!(table.plan.contract.partition_keys[0].as_str(), "month");
    for column in &table.plan.contract.columns {
        let spelling = match column.name.as_str() {
            "int_value" => "int64",
            "bool_value" => "bool",
            "date_value" | "partition_date" => "date32",
            "datetime_value" => "timestamp(us,UTC)",
            "decimal_value" => "decimal128(38,10)",
            _ => "utf8",
        };
        assert_eq!(
            column.logical_type,
            grv_types::logical::authoring_type(spelling).unwrap()
        );
    }
}

fn verify_rows(h: &Harness, selection: &str, oracle: &Value) -> Identities {
    verify_rows_at(h, selection, 1, oracle, &oracle[selection])
}

fn verify_rows_at(
    h: &Harness,
    selection: &str,
    revision: u64,
    oracle: &Value,
    expected_rows: &Value,
) -> Identities {
    let store = Store::open(LocalBackend::open(&h.root).unwrap()).unwrap();
    let selected = source::verify(
        &store,
        &Name::new(dataset(selection)).unwrap(),
        &RequestedRevision::Revision(U64::new(revision).unwrap()),
        &[TableSelection {
            table: Name::new("grvfix").unwrap(),
            partitions: None,
            expect_columns: None,
            expect_partition_keys: None,
            // Complete-empty revision deliberately has no files to infer schema.
            // Use the independently accepted base capture's complete contract.
            prior_source_contract: if expected_rows.as_object().unwrap().is_empty() {
                let journal: Value = serde_json::from_slice(
                    &fs::read(h.state.join("push").join("mutation-base-contract.json")).unwrap(),
                )
                .unwrap();
                let capture: grv_core::capture::CaptureReceipt =
                    serde_json::from_value(journal["evidence"]["capture"].clone()).unwrap();
                Some(capture.tables[0].plan.contract.clone())
            } else {
                None
            },
        }],
        h.temp.path(),
    )
    .unwrap();
    assert_eq!(selected.tables.len(), 1);
    assert_eq!(selected.tables[0].table.as_str(), "grvfix");
    assert_eq!(selected.revision.get(), revision);
    let expected = expected_rows.as_object().unwrap();
    // The empty contract is the same full projection as base; zero rows must
    // not erase its layout/schema. Check both metadata and independent reader.
    let shape = if selection == "empty" {
        "base"
    } else {
        selection
    };
    let mut names: BTreeSet<String> = oracle[shape]["0"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    names.extend(["id".to_owned(), "rel_id".to_owned()]);
    assert_eq!(
        selected.tables[0]
            .contract
            .columns
            .iter()
            .map(|column| column.name.to_string())
            .collect::<BTreeSet<_>>(),
        names
    );
    assert_eq!(
        selected.tables[0]
            .contract
            .partition_keys
            .iter()
            .map(|key| key.as_str())
            .collect::<Vec<_>>(),
        ["month"]
    );
    let mut identities = Identities::default();
    let mut months = BTreeMap::<String, usize>::new();
    let mut row_count = 0;
    for file in &selected.files {
        assert_eq!(file.access, grv_adapter_api::FileAccess::Local);
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&file.location).unwrap())
                .unwrap();
        assert_eq!(
            builder
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<BTreeSet<_>>(),
            names
        );
        assert_eq!(builder.schema().fields().len(), names.len());
        for field in builder.schema().fields() {
            assert_eq!(
                field.data_type(),
                &column_type(field.name()),
                "{}",
                field.name()
            );
        }
        let reader = builder.with_batch_size(257).build().unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            for index in 0..batch.num_rows() {
                let mut actual = serde_json::Map::new();
                for (column, field) in batch.schema().fields().iter().enumerate() {
                    assert!(
                        actual
                            .insert(field.name().clone(), scalar(&batch, column, index))
                            .is_none()
                    );
                }
                let name = actual["name"].as_str().unwrap();
                let i: usize = name.strip_prefix("GRVFIX-").unwrap().parse().unwrap();
                assert_eq!(name, format!("GRVFIX-{i:05}"));
                assert!(i < 1000 && i % 10 != 7, "unexpected selected row {i}");
                let id = record_id(&actual.remove("id").unwrap());
                assert!(identities.main.insert(i, id).is_none(), "duplicate row {i}");
                let rel_id = actual.remove("rel_id").unwrap();
                if i < 10 {
                    assert!(identities.related.insert(i, record_id(&rel_id)).is_none());
                } else {
                    assert!(rel_id.is_null(), "unexpected lookup for row {i}");
                }
                let month = actual["_month_"].as_str().unwrap().to_owned();
                assert_eq!(file.partition, json!({"month": month}));
                *months.entry(month).or_default() += 1;
                assert_eq!(
                    &Value::Object(actual),
                    &expected[&i.to_string()],
                    "{selection} row {i}"
                );
                row_count += 1;
            }
        }
    }
    assert_eq!(row_count, expected.len());
    assert_eq!(
        identities.main.keys().copied().collect::<BTreeSet<_>>(),
        expected
            .keys()
            .map(|key| key.parse::<usize>().unwrap())
            .collect()
    );
    assert_eq!(
        identities.main.values().collect::<BTreeSet<_>>().len(),
        row_count,
        "duplicate main Id"
    );
    assert_eq!(
        identities.related.values().collect::<BTreeSet<_>>().len(),
        identities.related.len(),
        "duplicate related Id"
    );
    assert!(
        identities
            .related
            .values()
            .all(|id| !identities.main.values().any(|main| main == id))
    );
    let mut expected_months = BTreeMap::<String, usize>::new();
    for row in expected.values() {
        *expected_months
            .entry(row["_month_"].as_str().unwrap().to_owned())
            .or_default() += 1;
    }
    assert_eq!(months, expected_months);
    assert_eq!(
        identities.related.len(),
        expected
            .keys()
            .filter(|key| key.parse::<usize>().unwrap() < 10)
            .count()
    );
    if expected.is_empty() {
        assert!(
            selected.files.is_empty(),
            "empty selection emitted data files"
        );
        assert!(selected.tables[0].partitions.is_empty());
    }
    identities
}

// Deliberately suppress child output: org/auth diagnostics must never leak tokens.
// Nonzero status remains a hard failure, including reset failures.
fn fixture_python(environment: &Environment, script: &str, reset: bool) -> Result<(), String> {
    assert_eq!(environment.org, AUTHORIZED_ORG);
    assert_eq!(environment.expected, AUTHORIZED_ID);
    let mut command = Command::new("python3");
    command.arg("-B").arg(fixture().join(script)).args([
        "--org",
        &environment.org,
        "--expected-org-id",
        &environment.expected,
    ]);
    if reset {
        command.arg("--ignore-storage-limit");
    }
    let output = command
        .output()
        .map_err(|_| format!("{script}: failed to start"))?;
    // These reviewed scripts never print auth responses/tokens. Preserve
    // their diagnostics privately for reset failures instead of discarding
    // the only explanation. Never render the child output in public results.
    if let Some(directory) = std::env::var_os("GRV_SALESFORCE_TEST_EVIDENCE_DIR") {
        let directory = PathBuf::from(directory);
        assert!(directory.is_absolute() && directory.is_dir());
        let path = directory.join(format!("{script}-{}.log", grv_types::Uuid::v4()));
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        file.write_all(&output.stdout).unwrap();
        file.write_all(&output.stderr).unwrap();
        file.sync_all().unwrap();
        eprintln!("private fixture diagnostics: {}", path.display());
    }
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{script}: failed (exit {:?}); child output withheld",
            output.status.code()
        ))
    }
}

struct FixtureReset<'a> {
    environment: &'a Environment,
    armed: bool,
}

impl FixtureReset<'_> {
    fn restore(&mut self) -> Result<(), String> {
        // provision.py performs clean deletion/reload and strict ALL-row readback.
        let result = fixture_python(self.environment, "provision.py", true);
        // A failed reset is reported, never converted into success or retried
        // silently from Drop. Keep the original panic when already unwinding.
        self.armed = false;
        result
    }
}

impl Drop for FixtureReset<'_> {
    fn drop(&mut self) {
        if self.armed {
            eprintln!("restoring pinned disposable fixture (may take ten minutes)");
            if let Err(error) = self.restore() {
                if std::thread::panicking() {
                    eprintln!("CLEANUP FAILED while preserving original test panic: {error}");
                } else {
                    panic!("CLEANUP FAILED: {error}");
                }
            }
        }
    }
}

#[test]
#[ignore = "writes/reset pinned disposable org; requires GRV_SALESFORCE_TEST_MUTATION=allow and transport=rest; reset may take ten minutes"]
fn production_cli_fixture_mutation_omission_and_restored_oracle() {
    assert_eq!(
        std::env::var("GRV_SALESFORCE_TEST_MUTATION").as_deref(),
        Ok("allow")
    );
    // Reject auto/Bulk before the harness can acquire any source session.
    assert_eq!(
        std::env::var("GRV_SALESFORCE_TEST_TRANSPORT").as_deref(),
        Ok("rest")
    );
    let oracle = oracle();
    let h = Harness::new();
    let base = h.capture("base", &oracle);
    fs::copy(
        h.state
            .join("push")
            .join(base.first_attempt.as_str())
            .join("journal.json"),
        h.state.join("push").join("mutation-base-contract.json"),
    )
    .unwrap();
    // Guard is armed BEFORE the first PATCH (a partial mutation also resets).
    // Every CLI child and Python source session exits before reset begins.
    let mut reset = FixtureReset {
        environment: &h.environment,
        armed: true,
    };
    fixture_python(&h.environment, "mutate.py", false).unwrap();
    let mut changed = oracle["base"].clone();
    assert!(changed.as_object_mut().unwrap().remove("0").is_some());
    // Move i=1 from August (2026-08-02) to September, rather than
    // assuming its original month. Date32 is independently a fixed literal.
    changed["1"]["partition_date"] = json!(20697);
    changed["1"]["_month_"] = json!("2026-09");
    let mutation_attempt = grv_types::Uuid::v4();
    let mutated = h.push(&base.decl, &mutation_attempt, None);
    assert_eq!(mutated["ok"], true, "{mutated}");
    assert_eq!(mutated["result"]["replayed"], false);
    assert_eq!(mutated["result"]["outcome"]["kind"], "published");
    // The fresh no-op in capture() reserved high-water 2 without revision 2.
    assert_eq!(mutated["result"]["outcome"]["revision"], "3");
    let changed_ids = verify_rows_at(&h, "base", 3, &oracle, &changed);
    let mut surviving = base.identities;
    surviving.main.remove(&0);
    surviving.related.remove(&0);
    assert_eq!(changed_ids, surviving);

    // Change ONLY the approved source filter, retaining the same dataset and
    // complete projection. Fresh attempt prevents changed-request replay.
    let declaration = fs::read_to_string(&base.decl).unwrap();
    let filter = "      filter: GrvStatus__c = 'Active'";
    assert_eq!(declaration.matches(filter).count(), 1);
    let empty_decl = h.temp.path().join("push-base-complete-empty.yml");
    fs::write(
        &empty_decl,
        declaration.replace(
            filter,
            "      filter: GrvStatus__c = 'Active' AND Name = 'GRVFIX-EMPTY-NONEXISTENT'",
        ),
    )
    .unwrap();
    let empty_attempt = grv_types::Uuid::v4();
    assert_ne!(empty_attempt, mutation_attempt);
    let empty = h.push(&empty_decl, &empty_attempt, None);
    assert_eq!(empty["ok"], true, "{empty}");
    assert_eq!(empty["result"]["replayed"], false);
    assert_ne!(empty["result"]["run_id"], mutated["result"]["run_id"]);
    assert_eq!(empty["result"]["outcome"]["kind"], "published");
    assert_eq!(empty["result"]["outcome"]["revision"], "4");
    verify_empty_capture(&h, &empty_attempt);
    verify_rows_at(&h, "base", 4, &oracle, &json!({}));
    // Independently decode revision state, not just the source selection.
    let store = Store::open(LocalBackend::open(&h.root).unwrap()).unwrap();
    let revision = grv_core::revision::read(
        &store,
        &Name::new(dataset("base")).unwrap(),
        grv_storage::model::Counter::new(4).unwrap(),
        h.temp.path(),
    )
    .unwrap();
    assert!(
        revision.state.is_empty(),
        "complete empty capture retained partitions"
    );
    drop(store);

    eprintln!("restoring pinned disposable fixture (may take ten minutes)");
    reset.restore().unwrap();
    let restored = h.push(&base.decl, &grv_types::Uuid::v4(), None);
    assert_eq!(restored["ok"], true, "{restored}");
    assert_eq!(restored["result"]["replayed"], false);
    assert_eq!(restored["result"]["outcome"]["kind"], "published");
    assert_eq!(restored["result"]["outcome"]["revision"], "5");
    // Full independent pristine values, not old Salesforce record identities:
    // clean reload is allowed (and expected) to allocate new record IDs.
    verify_rows_at(&h, "base", 5, &oracle, &oracle["base"]);
}

#[test]
#[ignore = "requires authorized disposable org, expected ID, python3 and production adapter executable"]
fn production_cli_org_capture_publication_and_source_free_terminal_replay() {
    let oracle = oracle();
    let h = Harness::new();
    assert_ne!(
        h.environment.transport, "bulk",
        "use production_cli_rich_bulk_contract_refusal; rich Bulk acquisition is not claimed"
    );
    let base = h.capture("base", &oracle);
    h.terminal_replays(&[base]);
}

#[test]
#[ignore = "requires authorized disposable org, expected ID, python3 and production adapter executable"]
fn production_cli_empty_capture_noop_and_source_free_terminal_replay() {
    let oracle = oracle();
    let h = Harness::new();
    assert_ne!(
        h.environment.transport, "bulk",
        "use production_cli_rich_bulk_contract_refusal for the complete empty contract"
    );
    let empty = h.capture("empty", &oracle);
    h.terminal_replays(&[empty]);
}

#[test]
#[ignore = "requires authorized disposable org, expected ID, python3 and production adapter executable"]
fn production_cli_relationship_acquisition_or_precise_bulk_refusal() {
    let oracle = oracle();
    let h = Harness::new();
    if h.environment.transport == "bulk" {
        h.bulk_refusal("rel");
        return;
    }
    let base = h.capture("base", &oracle);
    let rel = h.capture("rel", &oracle);
    // No live IDs are hardcoded. The same selected main rows and lookup
    // identities must survive both contracts and both fresh no-op attempts.
    assert_eq!(base.identities, rel.identities);
    h.terminal_replays(&[base, rel]);
}

#[test]
#[ignore = "requires authorized disposable org, expected ID and production adapter; transport=bulk"]
fn production_cli_rich_bulk_contract_refusal() {
    let h = Harness::new();
    for selection in ["base", "empty", "rel"] {
        h.bulk_refusal(selection);
    }
}

#[test]
#[ignore = "requires authorized disposable org, expected ID and production adapter executable"]
fn production_wrong_expected_org_authentication_exposes_no_source_session() {
    let environment = Environment::explicit();
    // Authentication-only negative guard: no local GRV root, CLI push, source
    // query, or Bulk job. Verification GETs are allowed, service effects are not.
    let correct = format!("salesforce:{}", environment.expected);
    assert_eq!(environment.authenticate(&correct).unwrap(), correct);
    let wrong = "salesforce:00D000000000001EAA";
    assert_ne!(wrong, correct);
    let error = environment.authenticate(wrong).unwrap_err();
    assert_eq!(error.code, "REQUEST_MISMATCH");
    assert_eq!(
        error.message,
        "authenticated org differs from fixed org identity"
    );
}
