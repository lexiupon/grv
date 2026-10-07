// A trusted scanner reads only its retained regular read-only descriptor.
// No synthetic /dev/fd pathname is opened, stat'ed, or passed to local globbing.
#include "duckdb/common/file_system.hpp"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
namespace {
struct VerifiedDescriptorState {
    std::mutex mutex;
    int descriptor = -1;
    string original, unique;
    uint64_t sequence = 0;
};
struct VerifiedDescriptorHandle final : FileHandle {
    int descriptor;
    idx_t position = 0;
    VerifiedDescriptorHandle(FileSystem &fs, string path, FileOpenFlags flags, int descriptor)
        : FileHandle(fs,std::move(path),flags),descriptor(descriptor) {}
    void Close() override { if (descriptor >= 0) { ::close(descriptor); descriptor = -1; } }
    ~VerifiedDescriptorHandle() override { Close(); }
};
struct VerifiedDescriptorFileSystem final : FileSystem {
    std::shared_ptr<VerifiedDescriptorState> state;
    explicit VerifiedDescriptorFileSystem(std::shared_ptr<VerifiedDescriptorState> state) : state(std::move(state)) {}
    string GetName() const override { return "grv_verified_descriptor"; }
    bool IsManuallySet() override { return true; }
    bool CanHandleFile(const string &path) override {
        return path.compare(0,8,"/dev/fd/") == 0 || path.compare(0,18,"grv-verified-fd://") == 0;
    }
    void Require(const string &path) {
        if (state->descriptor < 3 || (path != state->original && path != state->unique) || FileSystem::HasGlob(path)) {
            throw PermissionException("verified scanner rejects unauthorized descriptor, directory or glob");
        }
        struct stat metadata;
        if (::fstat(state->descriptor,&metadata) != 0 || !S_ISREG(metadata.st_mode)) {
            throw PermissionException("verified scanner lost its retained regular descriptor");
        }
    }
    bool FileExists(const string &path, optional_ptr<FileOpener> = nullptr) override {
        std::lock_guard<std::mutex> guard(state->mutex); Require(path); return true;
    }
    bool DirectoryExists(const string &path, optional_ptr<FileOpener> = nullptr) override {
        std::lock_guard<std::mutex> guard(state->mutex); Require(path); return false;
    }
    bool IsPipe(const string &path, optional_ptr<FileOpener> = nullptr) override {
        std::lock_guard<std::mutex> guard(state->mutex); Require(path); return false;
    }
    vector<OpenFileInfo> Glob(const string &path, FileOpener * = nullptr) override {
        std::lock_guard<std::mutex> guard(state->mutex); Require(path);
        // Each authorized scan gets a distinct cache identity, even when the OS
        // reuses an fd number for a later file. Never expand a pattern.
        return {OpenFileInfo(state->unique)};
    }
    unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> = nullptr) override {
        std::lock_guard<std::mutex> guard(state->mutex); Require(path);
        if (!flags.OpenForReading() || flags.OpenForWriting() || flags.CreateFileIfNotExists()
            || flags.OverwriteExistingFile() || flags.OpenForAppending() || flags.ExclusiveCreate()
            || flags.Lock() == FileLockType::WRITE_LOCK) {
            throw PermissionException("verified scanner only permits read-only opens");
        }
        int held = ::fcntl(state->descriptor,F_DUPFD_CLOEXEC,3);
        if (held < 0) { throw IOException("verified scanner could not retain its descriptor"); }
        return make_uniq<VerifiedDescriptorHandle>(*this,path,flags,held);
    }
    VerifiedDescriptorHandle &Checked(FileHandle &handle) {
        std::lock_guard<std::mutex> guard(state->mutex); Require(handle.path);
        auto &result = handle.Cast<VerifiedDescriptorHandle>();
        if (result.descriptor < 3) { throw IOException("verified descriptor is closed"); }
        return result;
    }
    int64_t ReadAt(FileHandle &handle, void *buffer, int64_t bytes, idx_t location) {
        auto &file = Checked(handle);
        if (bytes < 0 || location > uint64_t(std::numeric_limits<off_t>::max())
            || uint64_t(bytes) > uint64_t(std::numeric_limits<off_t>::max())-location) {
            throw IOException("verified descriptor read is out of range");
        }
        ssize_t count;
        do { count = ::pread(file.descriptor,buffer,size_t(bytes),off_t(location)); } while (count < 0 && errno == EINTR);
        if (count < 0) { throw IOException("verified descriptor read failed"); }
        return count;
    }
    void Read(FileHandle &handle, void *buffer, int64_t bytes, idx_t location) override {
        int64_t done = 0;
        while (done < bytes) {
            auto count = ReadAt(handle,static_cast<char *>(buffer)+done,bytes-done,location+done);
            if (!count) { throw IOException("verified descriptor ended before the requested range"); }
            done += count;
        }
    }
    int64_t Read(FileHandle &handle, void *buffer, int64_t bytes) override {
        auto &file = Checked(handle);
        auto count = ReadAt(handle,buffer,bytes,file.position); file.position += count; return count;
    }
    struct stat Metadata(FileHandle &handle) {
        auto &file = Checked(handle); struct stat result;
        if (::fstat(file.descriptor,&result) != 0 || !S_ISREG(result.st_mode)) { throw IOException("verified descriptor metadata unavailable"); }
        return result;
    }
    int64_t GetFileSize(FileHandle &handle) override { return Metadata(handle).st_size; }
    FileType GetFileType(FileHandle &handle) override { Metadata(handle); return FileType::FILE_TYPE_REGULAR; }
    timestamp_t GetLastModifiedTime(FileHandle &handle) override {
        auto metadata = Metadata(handle);
#ifdef __APPLE__
        auto time = metadata.st_mtimespec;
#else
        auto time = metadata.st_mtim;
#endif
        return timestamp_t(int64_t(time.tv_sec)*1000000+time.tv_nsec/1000);
    }
    string GetVersionTag(FileHandle &handle) override {
        auto metadata = Metadata(handle);
        return std::to_string(metadata.st_dev)+":"+std::to_string(metadata.st_ino)+":"+std::to_string(metadata.st_size)+":"+std::to_string(GetLastModifiedTime(handle).value);
    }
    FileMetadata Stats(FileHandle &handle) override {
        FileMetadata result; result.file_size = GetFileSize(handle);
        result.file_type = FileType::FILE_TYPE_REGULAR; result.last_modification_time = GetLastModifiedTime(handle); return result;
    }
    bool CanSeek() override { return true; }
    bool OnDiskFile(FileHandle &handle) override { Checked(handle); return true; }
    void Seek(FileHandle &handle, idx_t location) override { Checked(handle).position = location; }
    idx_t SeekPosition(FileHandle &handle) override { return Checked(handle).position; }
    void Reset(FileHandle &handle) override { Seek(handle,0); }
};
}
