#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <ranges>
#include <spanstream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

///
/// @brief A move-only wrapper for a type T. (no copies allowed).
///
template <typename T, T empty = T{}> class MoveOnly final {
public:
  MoveOnly() noexcept = default;
  constexpr MoveOnly(const T value) noexcept : value_{value} {}
  constexpr MoveOnly(MoveOnly &&other) noexcept
      : value_{std::exchange(other.value_, empty)} {}

  constexpr auto operator=(MoveOnly &&other) noexcept -> MoveOnly & {
    value_ = std::exchange(other.value_, empty);
    return *this;
  }

  [[nodiscard]] constexpr explicit operator T() const noexcept { return get(); }
  [[nodiscard]] constexpr auto get() const noexcept -> T { return value_; }

private:
  T value_{empty};
};

///
/// @brief Represents a file descriptor.
///
class FileDescriptor final {
public:
  explicit FileDescriptor(const std::filesystem::path &path)
      : id_{open(path.c_str(), O_RDONLY)} {
    if (id_.get() == -1) {
      std::cerr << "Failed to open file: " << path << "\n";
      std::exit(EXIT_FAILURE);
    }
  }

  ~FileDescriptor() noexcept {
    if (id_.get() != -1) {
      close(id_.get());
    }
  }

  [[nodiscard]] auto get() const noexcept -> int { return id_.get(); }

private:
  MoveOnly<int, -1> id_;
};

///
/// @brief Gets the size of the file associated with the given file descriptor.
/// @param fd The file descriptor.
/// @return The size of the file in bytes.
///
inline auto get_file_size(const FileDescriptor &fd) noexcept -> std::size_t {
  struct ::stat sb{};
  if (::fstat(fd.get(), &sb) == -1) {
    std::cerr << "Failed to get file size\n";
    std::exit(EXIT_FAILURE);
  }
  return sb.st_size;
}

///
/// @brief Gets the beginning of the memory mapped file.
/// @param fd The file descriptor.
/// @param size The size of the file.
/// @return Pointer to the beginning of the mapped file.
///
inline auto get_beginning_of_file(const FileDescriptor &fd,
                                  const std::size_t size) noexcept -> char * {
  void *mapped = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd.get(), 0);

  if (mapped == MAP_FAILED) {
    std::cerr << "Failed to map file to memory\n";
    std::exit(EXIT_FAILURE);
  }

  return static_cast<char *>(mapped);
}

///
/// @brief Represents a memory-mapped file.
///
class MappedFile final {
public:
  explicit MappedFile(const std::filesystem::path &path) noexcept
      : fd_{path}, size_{get_file_size(fd_)},
        begin_{get_beginning_of_file(fd_, size_.get())} {}

  ~MappedFile() noexcept {
    if (begin_.get() == nullptr) {
      return;
    }

    ::munmap(begin_.get(), size_.get());
  }

  ///
  /// @brief Gets the data of the mapped file as a span of const chars.
  /// @return A span representing the file data.
  ///
  [[nodiscard]] auto data() const noexcept -> std::span<const char> {
    return {begin_.get(), size_.get()};
  }

private:
  FileDescriptor fd_;
  MoveOnly<size_t> size_;
  MoveOnly<char *> begin_;
};

struct Record {
  std::uint64_t count{0};
  float sum{0.0F};
  float min{0.0F};
  float max{0.0F};
};

using Database = std::unordered_map<std::string, Record>;

///
/// @brief Processes the input data from the given input stream.
/// @param in The input stream to read data from.
/// @return A database containing the processed records.
///
auto process_raw_data(std::istream &in) -> Database {
  Database db;

  std::string station;
  std::string value_string;

  while (std::getline(in, station, ';') &&
         std::getline(in, value_string, '\n')) {
    float value;
    std::from_chars(value_string.data(),
                    value_string.data() + value_string.size(), value);

    auto it = db.find(station);
    if (it == db.end()) {
      db.emplace(station,
                 Record{.count = 1, .sum = value, .min = value, .max = value});
      continue;
    }

    it->second.min = std::min(it->second.min, value);
    it->second.max = std::max(it->second.max, value);
    it->second.sum += value;
    ++it->second.count;
  }
  return db;
}

///
/// @brief Finds the boundary of a chunk by locating the next newline character.
/// @param chunk The data chunk as a span of const chars.
/// @param pos The starting position to search from.
/// @return The position of the chunk boundary (after the newline).
///
constexpr auto find_chunk_boundary(const std::span<const char> &chunk,
                                   std::size_t pos) noexcept -> std::size_t {
  while (pos < chunk.size() && chunk[pos] != '\n') {
    ++pos;
  }
  return pos + 1; // Include the newline.
}

///
/// @brief Merges the source database into the destination database.
/// @param dest The destination database to merge into.
/// @param src The source database to merge from.
///
auto merge_databases(Database &dest, const Database &src) noexcept -> void {
  for (const auto &[station, record] : src) {
    auto it = dest.find(station);
    if (it == dest.end()) {
      dest.emplace(station, record);
      continue;
    }

    it->second.min = std::min(it->second.min, record.min);
    it->second.max = std::max(it->second.max, record.max);
    it->second.sum += record.sum;
    it->second.count += record.count;
  }
}

///
/// @brief Processes the data in parallel using multiple threads.
/// @param data The input data as a span of const chars.
/// @param thread_count The number of threads to use for processing.
/// @return A database containing the processed records.
///
auto process_data_parallel(const std::span<const char> &data,
                           const std::size_t thread_count) noexcept
    -> Database {
  std::vector<Database> partial_dbs(thread_count);
  std::vector<std::thread> threads;
  const auto chunk_size = data.size() / thread_count;

  for (std::size_t i = 0; i < thread_count; ++i) {
    const auto start = i * chunk_size;
    auto end = (i == thread_count - 1)
                   ? data.size()
                   : find_chunk_boundary(data, (i + 1) * chunk_size);

    threads.emplace_back([&, start, end, i]() {
      std::ispanstream in{data.subspan(start, end - start)};
      partial_dbs[i] = process_raw_data(in);
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  Database final_db;
  for (const auto &partial_db : partial_dbs) {
    merge_databases(final_db, partial_db);
  }

  return final_db;
}

///
/// @brief Prints the database records to the specified output stream.
/// @param db The database to print.
/// @param out The output stream to print to (default is std::cout).
///
auto print(const Database &db, std::ostream &out = std::cout) noexcept -> void {
  std::vector<std::string> stations{db.size()};
  std::ranges::copy(db | std::ranges::views::keys, stations.begin());
  std::ranges::sort(stations);

  out << std::setiosflags(std::ostream::fixed | std::ostream::showpoint)
      << std::setprecision(1) << "{";

  for (auto &station : stations) {
    const auto &record = db.at(station);
    out << station << ": " << record.min << " / "
        << record.sum / static_cast<float>(record.count) << " / " << record.max
        << ",\n";
  }
}

///
/// @brief Main entry point of the program.
/// @param argc Argument count.
/// @param argv Argument vector.
/// @return Exit status code.
///
auto main(const int argc, const char **argv) noexcept -> int {
  const MappedFile mapped_file{"../../data/measurements.txt"};
  const auto db{process_data_parallel(mapped_file.data(),
                                      std::thread::hardware_concurrency())};
  print(db);

  return 0;
}
