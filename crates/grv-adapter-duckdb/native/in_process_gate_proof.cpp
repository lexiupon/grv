// Source-including bounded synthetic gate proof. No transport, credentials or
// network. It models the in-process owner contract at the NATIVE boundary only:
// a genuinely in-flight, controlled blocked native operation (a real WAL
// FileSync at COMMIT held behind a bounded gate) while a cooperative interrupt
// is requested. The interrupt is a request only: it cannot stop a synchronous
// operation already in flight.
//
// LIMITATION (labelled, not hidden): this harness opens the engine directly and
// takes a MANUAL flock on a sidecar file. It is NOT the Rust WorkspaceLock and
// NOT the SDK stopped-acknowledgement path; grv_native_close here destroys the
// engine on the calling (owner) thread, which is a synchronous return plus
// destruction, not a thread join. The actual SDK stop/ownership/join evidence
// for the same contract is supplied separately by the in-repo
// commit_boundary_tests (real SDK child, real workspace lock, no stopped ack
// before join). This proof only establishes the native-side facts: exclusion
// is retained while the operation is in flight, the cooperative interrupt does
// not stop it, and a fresh invocation is admitted only after the owner closes.
#include "shim.cpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <chrono>
#include <iostream>
#include <limits>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <dirent.h>

namespace {
struct GateState {
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic<bool> armed{false};
    // True only while the owner thread is inside the bounded gate; reset on exit.
    std::atomic<bool> in_gate{false};
    // Set by the owner immediately after the gated synchronous call returns.
    std::atomic<bool> commit_returned{false};
    std::atomic<bool> released{false};
    std::atomic<bool> parent_ok{true};
    std::atomic<int> sync_calls{0};
    std::atomic<int> wal_sync_calls{0};
    std::atomic<int> commit_status{-99};
    // Parent observations.
    std::atomic<bool> lock_held_while_in_flight{false};
    std::atomic<bool> still_in_gate_and_unreturned_after_interrupt{false};
    // Scenario 2 observations.
    std::atomic<bool> gate_entered_scenario2{false};
};
struct GateFileSystem : LocalFileSystem {
    std::shared_ptr<GateState> state;
    explicit GateFileSystem(std::shared_ptr<GateState> state) : state(std::move(state)) {}
    void FileSync(FileHandle &handle) override {
        state->sync_calls.fetch_add(1, std::memory_order_relaxed);
        bool wal = StringUtil::EndsWith(handle.path, ".wal");
        if (wal) {
            state->wal_sync_calls.fetch_add(1, std::memory_order_relaxed);
        }
        if (state->armed.load(std::memory_order_acquire) && wal) {
            state->in_gate.store(true, std::memory_order_release);
            {
                std::unique_lock<std::mutex> lock(state->mutex);
                // Bounded wait: the gate can never hold the owner forever, even
                // if the observer thread fails before releasing.
                state->cv.wait_for(lock, std::chrono::seconds(10),
                                    [&] { return state->released.load(std::memory_order_acquire); });
            }
            state->in_gate.store(false, std::memory_order_release); // gate exited
            // The gate is released; the operation proceeds normally. The
            // cooperative interrupt requested while in flight did not stop it.
        }
        LocalFileSystem::FileSync(handle);
    }
};
Owner *GateOwner(const char *path, std::shared_ptr<GateState> state) {
    DBConfig config;
    config.file_system = make_uniq<VirtualFileSystem>(make_uniq<GateFileSystem>(state));
    config.SetOptionByName("threads", Value(int64_t(1)));
    config.SetOptionByName("enable_external_access", Value(false));
    config.SetOptionByName("autoload_known_extensions", Value(false));
    config.SetOptionByName("autoinstall_known_extensions", Value(false));
    auto engine = std::make_unique<Engine>(path, config);
    engine->database.LoadStaticExtension<ParquetExtension>();
    engine->connection.context->config.use_replacement_scans = false;
    shared_ptr<GRVGuard> policy;
    engine->connection.context->RunFunctionInTransaction([&] { policy = Policy(*engine->connection.context); });
    engine->connection.context->registered_state->Insert("grv.native.guard.v1", policy);
    auto interrupt = std::make_shared<GRVInterruptControl>();
    interrupt->engine = engine.get();
    return new Owner{std::move(engine), std::move(interrupt)};
}
struct LockHandle {
    int fd = -1;
    bool Acquire(const char *path) {
        fd = ::open(path, O_RDWR | O_CREAT, 0600);
        if (fd < 0) {
            return false;
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            ::close(fd);
            fd = -1;
            return false;
        }
        return true;
    }
    void Release() {
        if (fd >= 0) {
            ::flock(fd, LOCK_UN);
            ::close(fd);
            fd = -1;
        }
    }
    ~LockHandle() {
        Release();
    }
};
bool WaitUntil(const std::function<bool()> &probe, std::chrono::milliseconds bound) {
    auto deadline = std::chrono::steady_clock::now() + bound;
    while (!probe()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}
// RAII: every observer exit (normal or exception) releases and notifies, so
// the owner thread can never hang on the gate.
struct ReleaseGuard {
    std::shared_ptr<GateState> state;
    ~ReleaseGuard() {
        state->released.store(true, std::memory_order_release);
        state->cv.notify_all();
    }
};
// Decodes the bounded metadata bridge encoding: u64 rows, u64 columns, then
// per cell one validity byte, an eight-byte little-endian text length, and the
// text bytes. The decoded cell must be a non-negative decimal integer.
struct DecodedScalar {
    bool ok = false;
    int64_t value = 0;
    uint64_t rows = 0;
    uint64_t columns = 0;
};
DecodedScalar RunScalarQuery(void *owner, const char *sql) {
    DecodedScalar out;
    unsigned char bytes[65536];
    size_t written = 0;
    char error[4096] = {0};
    if (grv_native_metadata_query(owner, sql, bytes, sizeof(bytes), &written, error, sizeof(error))) {
        return out;
    }
    if (written < 25) {
        return out;
    }
    auto u64le = [&](size_t offset) {
        uint64_t v = 0;
        for (size_t i = 0; i < 8; ++i) {
            v |= uint64_t(bytes[offset + i]) << (8 * i);
        }
        return v;
    };
    out.rows = u64le(0);
    out.columns = u64le(8);
    if (out.rows != 1 || out.columns != 1) {
        return out;
    }
    if (bytes[16] != 1) {
        return out; // NULL cell is not the expected committed value
    }
    size_t offset = 17;
    size_t length = u64le(offset);
    offset += 8;
    if (length != written - offset) {
        return out;
    }
    int64_t v = 0;
    for (size_t i = 0; i < length; ++i) {
        char c = static_cast<char>(bytes[offset + i]);
        if (c < '0' || c > '9') {
            return out;
        }
        const auto digit = c - '0';
        if (v > (std::numeric_limits<int64_t>::max() - digit) / 10) {
            return out;
        }
        v = v * 10 + digit;
    }
    if (length == 0) {
        return out;
    }
    out.value = v;
    out.ok = true;
    return out;
}
void RemoveTree(const std::string &path) {
    struct dirent *entry = nullptr;
    auto dir = ::opendir(path.c_str());
    if (dir) {
        while ((entry = ::readdir(dir)) != nullptr) {
            if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) {
                continue;
            }
            std::string child = path + "/" + entry->d_name;
            struct stat st;
            if (::lstat(child.c_str(), &st) == 0) {
                if (S_ISDIR(st.st_mode)) {
                    RemoveTree(child);
                } else {
                    ::unlink(child.c_str());
                }
            }
        }
        ::closedir(dir);
    }
    ::rmdir(path.c_str());
}
}
int main() {
    std::string owned_root;
    try {
        char pattern[] = "/tmp/grv-in-process-gate-XXXXXX";
        auto base = ::mkdtemp(pattern);
        if (!base) {
            throw IOException("private directory");
        }
        owned_root = base;
        string root(base);
        auto query = [](void *owner, const char *sql) {
            unsigned char bytes[65536];
            size_t written = 0;
            char error[4096] = {0};
            return grv_native_metadata_query(owner, sql, bytes, sizeof(bytes), &written, error, sizeof(error));
        };
        // Scenario 1: a cooperative interrupt requested while the operation is
        // in flight does not stop it; the owner is not closed and the lock is
        // not released until the gate releases and the owner closes.
        {
            auto path = root + "/gate1.duckdb";
            auto lock_path = root + "/gate1.duckdb.grv-lock";
            auto state = std::make_shared<GateState>();
            auto *owner = GateOwner(path.c_str(), state);
            auto interrupt_handle = grv_native_interrupt_handle(owner);
            LockHandle held;
            if (!held.Acquire(lock_path.c_str())) {
                throw IOException("owner could not retain workspace exclusion");
            }
            if (query(owner, "CREATE TABLE _grv_gate(id BIGINT)") || query(owner, "BEGIN") ||
                query(owner, "INSERT INTO _grv_gate VALUES (7)")) {
                throw IOException("baseline");
            }
            state->armed.store(true, std::memory_order_release);
            std::thread parent([&] {
                ReleaseGuard guard{state};
                try {
                    if (!WaitUntil([&] { return state->in_gate.load(std::memory_order_acquire); },
                                    std::chrono::seconds(10))) {
                        state->parent_ok.store(false);
                        return;
                    }
                    // While the operation is in flight the owner retains exclusion.
                    LockHandle probe;
                    state->lock_held_while_in_flight.store(!probe.Acquire(lock_path.c_str()), std::memory_order_release);
                    // Request the supported cooperative interrupt; it cannot stop
                    // the synchronous operation already in flight.
                    grv_native_interrupt(interrupt_handle);
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    // The gate must still be held AND the call must not have
                    // returned: the interrupt is a request, not a stop.
                    state->still_in_gate_and_unreturned_after_interrupt
                        .store(state->in_gate.load(std::memory_order_acquire) &&
                                   !state->commit_returned.load(std::memory_order_acquire),
                               std::memory_order_release);
                    // Release happens in the guard destructor on every exit.
                } catch (...) {
                    state->parent_ok.store(false);
                }
            });
            state->commit_status.store(query(owner, "COMMIT"), std::memory_order_relaxed);
            state->commit_returned.store(true, std::memory_order_release); // set immediately after return
            grv_native_interrupt_close(interrupt_handle);
            // Synchronous return plus destruction on the owner thread; NOT a
            // thread join (the owner here is this thread).
            grv_native_close(owner);
            held.Release();
            parent.join();
            if (!state->parent_ok.load()) {
                throw IOException("parent observer failed");
            }
            if (!state->lock_held_while_in_flight.load()) {
                throw IOException("workspace exclusion was not retained while in flight");
            }
            if (!state->still_in_gate_and_unreturned_after_interrupt.load()) {
                throw IOException("interrupt stopped the in-flight operation; not a cooperative request");
            }
            LockHandle after;
            if (!after.Acquire(lock_path.c_str())) {
                throw IOException("workspace exclusion not released after unblock and owner close");
            }
            // The interrupted COMMIT may have persisted or not; record the
            // actual observed outcome without claiming rollback or commit.
            char error[4096] = {0};
            void *reopened = grv_native_open(path.c_str(), error, sizeof(error));
            if (!reopened) {
                throw IOException("fresh invocation refused after owner close");
            }
            if (grv_native_authorize_relation(reopened, "main", "_grv_gate", error, sizeof(error))) {
                throw IOException(string("fresh invocation could not authorize the relation: ") + error);
            }
            auto observed = RunScalarQuery(reopened, "SELECT count(*) FROM _grv_gate");
            std::cout << "scenario1 observed post-interrupt commit row count: "
                      << (observed.ok ? std::to_string(observed.value) : std::string("unresolved")) << "\n";
            grv_native_close(reopened);
        }
        // Scenario 2: with no interrupt, the gated COMMIT completes; the
        // immutable result survives and a fresh invocation is admitted after the
        // owner closes and the lock releases.
        {
            auto path = root + "/gate2.duckdb";
            auto lock_path = root + "/gate2.duckdb.grv-lock";
            auto state = std::make_shared<GateState>();
            auto *owner = GateOwner(path.c_str(), state);
            LockHandle held;
            if (!held.Acquire(lock_path.c_str())) {
                throw IOException("owner could not retain workspace exclusion");
            }
            if (query(owner, "CREATE TABLE _grv_gate(id BIGINT)") || query(owner, "BEGIN") ||
                query(owner, "INSERT INTO _grv_gate VALUES (9)")) {
                throw IOException("baseline");
            }
            state->armed.store(true, std::memory_order_release);
            std::thread parent([&] {
                ReleaseGuard guard{state};
                try {
                    if (!WaitUntil([&] { return state->in_gate.load(std::memory_order_acquire); },
                                    std::chrono::seconds(10))) {
                        state->parent_ok.store(false);
                        return;
                    }
                    state->gate_entered_scenario2.store(true, std::memory_order_release);
                    // Release happens in the guard destructor on every exit.
                } catch (...) {
                    state->parent_ok.store(false);
                }
            });
            state->commit_status.store(query(owner, "COMMIT"), std::memory_order_relaxed);
            state->commit_returned.store(true, std::memory_order_release);
            grv_native_close(owner);
            held.Release();
            parent.join();
            if (!state->parent_ok.load()) {
                throw IOException("parent observer failed");
            }
            if (!state->gate_entered_scenario2.load()) {
                throw IOException("bounded gate never entered in scenario 2");
            }
            if (state->commit_status.load() != 0) {
                throw IOException("gated commit did not succeed after the bounded gate released");
            }
            // A fresh invocation is admitted only after the previous owner
            // closed and released exclusion; it observes the immutable result.
            char error[4096] = {0};
            void *reopened = grv_native_open(path.c_str(), error, sizeof(error));
            if (!reopened) {
                throw IOException("fresh invocation refused after join");
            }
            if (grv_native_authorize_relation(reopened, "main", "_grv_gate", error, sizeof(error))) {
                throw IOException(string("fresh invocation could not authorize the committed relation: ") + error);
            }
            auto decoded = RunScalarQuery(reopened, "SELECT id FROM _grv_gate");
            if (!decoded.ok || decoded.value != 9) {
                throw IOException("fresh invocation did not read back the exact committed value");
            }
            grv_native_close(reopened);
        }
        std::cout << "bounded synthetic gate: in-flight exclusion, cooperative interrupt, owner close and fresh invocation passed\n";
        std::cout << "(native-boundary model: manual flock sidecar, not the Rust WorkspaceLock or the SDK stopped-ack path)\n";
        RemoveTree(owned_root);
        return 0;
    } catch (const std::exception &failure) {
        std::cerr << "in-process gate proof failed: " << failure.what() << "\n";
        if (!owned_root.empty()) {
            RemoveTree(owned_root);
        }
        return 1;
    }
}
