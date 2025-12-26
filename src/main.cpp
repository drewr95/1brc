#include <algorithm>
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
  std::string value;

  while (std::getline(in, station, ';') && std::getline(in, value, '\n')) {
    const auto val = std::stof(value);

    auto it = db.find(station);
    if (it == db.end()) {
      db.emplace(station,
                 Record{.count = 1, .sum = val, .min = val, .max = val});
      continue;
    }

    it->second.min = std::min(it->second.min, val);
    it->second.max = std::max(it->second.max, val);
    it->second.sum += val;
    ++it->second.count;
  }
  return db;
}

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
  const MappedFile mapped_file{"../../data/weather_stations.csv"};
  std::ispanstream input_stream{mapped_file.data()};
  const auto db{process_raw_data(input_stream)};
  print(db);

  return 0;
}
