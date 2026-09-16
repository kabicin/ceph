#pragma once

#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <thread>
#include <atomic>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "common/dout.h"

namespace rgw::lua {

using BackgroundMapValue = std::variant<std::string, long long int, double, bool>;
using BackgroundMap = std::unordered_map<std::string, BackgroundMapValue>;

enum class MapUpdateOp : uint8_t {
  SET       = 0x01,
  ERASE     = 0x02,
  INCREMENT = 0x03,
};

enum class MapValueType : uint8_t {
  STRING = 0x01,
  INT64  = 0x02,
  DOUBLE = 0x03,
  BOOL   = 0x04,
};

inline void append_u8(std::string& buf, uint8_t v) {
  buf.push_back(static_cast<char>(v));
}

inline void append_u16(std::string& buf, uint16_t v) {
  buf.push_back(static_cast<char>((v >> 8) & 0xff));
  buf.push_back(static_cast<char>(v & 0xff));
}

inline void append_u32(std::string& buf, uint32_t v) {
  buf.push_back(static_cast<char>((v >> 24) & 0xff));
  buf.push_back(static_cast<char>((v >> 16) & 0xff));
  buf.push_back(static_cast<char>((v >>  8) & 0xff));
  buf.push_back(static_cast<char>(v & 0xff));
}

template<typename T>
inline void append_raw(std::string& buf, T v) {
  static_assert(std::is_trivially_copyable_v<T>);
  char tmp[sizeof(T)];
  std::memcpy(tmp, &v, sizeof(T));
  buf.append(tmp, sizeof(T));
}

inline uint8_t read_u8(const char* p) {
  return static_cast<uint8_t>(p[0]);
}
inline uint16_t read_u16(const char* p) {
  return (static_cast<uint16_t>(static_cast<uint8_t>(p[0])) << 8)
       |  static_cast<uint16_t>(static_cast<uint8_t>(p[1]));
}
inline uint32_t read_u32(const char* p) {
  return (static_cast<uint32_t>(static_cast<uint8_t>(p[0])) << 24)
       | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 16)
       | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) <<  8)
       |  static_cast<uint32_t>(static_cast<uint8_t>(p[3]));
}
template<typename T>
inline T read_raw(const char* p) {
  static_assert(std::is_trivially_copyable_v<T>);
  T v;
  std::memcpy(&v, p, sizeof(T));
  return v;
}

inline bool write_all(int fd, const char* buf, size_t len) {
  while (len > 0) {
    ssize_t n = ::write(fd, buf, len);
    if (n <= 0) return false;
    buf += n;
    len -= static_cast<size_t>(n);
  }
  return true;
}

inline bool read_all(int fd, char* buf, size_t len) {
  while (len > 0) {
    ssize_t n = ::read(fd, buf, len);
    if (n <= 0) return false;
    buf += n;
    len -= static_cast<size_t>(n);
  }
  return true;
}

inline std::string build_set_msg(std::string_view key,
                                  const BackgroundMapValue& value) {
  std::string payload;
  append_u8(payload, static_cast<uint8_t>(MapUpdateOp::SET));
  append_u16(payload, static_cast<uint16_t>(key.size()));
  payload.append(key);

  std::visit([&payload](auto&& v) {
    using T = std::decay_t<decltype(v)>;
    if constexpr (std::is_same_v<T, std::string>) {
      append_u8(payload, static_cast<uint8_t>(MapValueType::STRING));
      append_u32(payload, static_cast<uint32_t>(v.size()));
      payload.append(v);
    } else if constexpr (std::is_same_v<T, long long int>) {
      append_u8(payload, static_cast<uint8_t>(MapValueType::INT64));
      append_raw(payload, static_cast<int64_t>(v));
    } else if constexpr (std::is_same_v<T, double>) {
      append_u8(payload, static_cast<uint8_t>(MapValueType::DOUBLE));
      append_raw(payload, v);
    } else if constexpr (std::is_same_v<T, bool>) {
      append_u8(payload, static_cast<uint8_t>(MapValueType::BOOL));
      append_u8(payload, v ? 1 : 0);
    }
  }, value);

  std::string frame;
  append_u32(frame, static_cast<uint32_t>(payload.size()));
  frame.append(payload);
  return frame;
}

inline std::string build_erase_msg(std::string_view key) {
  std::string payload;
  append_u8(payload, static_cast<uint8_t>(MapUpdateOp::ERASE));
  append_u16(payload, static_cast<uint16_t>(key.size()));
  payload.append(key);

  std::string frame;
  append_u32(frame, static_cast<uint32_t>(payload.size()));
  frame.append(payload);
  return frame;
}

inline std::string build_increment_msg(std::string_view key,
                                        const BackgroundMapValue& delta) {
  std::string payload;
  append_u8(payload, static_cast<uint8_t>(MapUpdateOp::INCREMENT));
  append_u16(payload, static_cast<uint16_t>(key.size()));
  payload.append(key);

  std::visit([&payload](auto&& v) {
    using T = std::decay_t<decltype(v)>;
    if constexpr (std::is_same_v<T, long long int>) {
      append_u8(payload, static_cast<uint8_t>(MapValueType::INT64));
      append_raw(payload, static_cast<int64_t>(v));
    } else if constexpr (std::is_same_v<T, double>) {
      append_u8(payload, static_cast<uint8_t>(MapValueType::DOUBLE));
      append_raw(payload, v);
    }
  }, delta);

  std::string frame;
  append_u32(frame, static_cast<uint32_t>(payload.size()));
  frame.append(payload);
  return frame;
}

inline int send_frame(const std::string& socket_path, const std::string& frame) {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return -errno;

  struct sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  ::strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

  if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    int e = errno;
    ::close(fd);
    return -e;
  }

  bool ok = write_all(fd, frame.data(), frame.size());
  ::close(fd);
  return ok ? 0 : -EIO;
}

inline int send_map_set(const std::string& socket_path,
                         std::string_view key,
                         const BackgroundMapValue& value) {
  return send_frame(socket_path, build_set_msg(key, value));
}

inline int send_map_erase(const std::string& socket_path,
                           std::string_view key) {
  return send_frame(socket_path, build_erase_msg(key));
}

inline int send_map_increment(const std::string& socket_path,
                               std::string_view key,
                               const BackgroundMapValue& delta) {
  return send_frame(socket_path, build_increment_msg(key, delta));
}

class MapUpdateServer {
public:
  MapUpdateServer() = default;

  MapUpdateServer(const MapUpdateServer&) = delete;
  MapUpdateServer& operator=(const MapUpdateServer&) = delete;

  int start(const std::string& path, BackgroundMap* map, std::mutex* mtx,
            const DoutPrefixProvider* dpp) {
    socket_path_ = path;
    map_ = map;
    mtx_ = mtx;
    dpp_ = dpp;

    ::unlink(path.c_str());

    listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return -errno;

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    ::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr),
               sizeof(addr)) < 0) {
      int e = errno;
      ::close(listen_fd_);
      listen_fd_ = -1;
      return -e;
    }

    if (::listen(listen_fd_, 64) < 0) {
      int e = errno;
      ::close(listen_fd_);
      listen_fd_ = -1;
      return -e;
    }

    running_ = true;
    thread_ = std::thread(&MapUpdateServer::run, this);
    return 0;
  }

  void stop() {
    if (!running_.exchange(false)) return;
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    if (thread_.joinable()) thread_.join();
    ::unlink(socket_path_.c_str());
  }

  ~MapUpdateServer() { stop(); }

  const std::string& socket_path() const { return socket_path_; }
  bool is_running() const { return running_.load(); }

private:
  void run() {
    while (running_) {
      int client_fd = ::accept(listen_fd_, nullptr, nullptr);
      if (client_fd < 0) {
        if (!running_) break;
        if (errno == EINTR) continue;
        ldpp_dout(dpp_, 1) << "MapUpdateServer: accept() error: "
                           << strerror(errno) << dendl;
        break;
      }
      handle_client(client_fd);
      ::close(client_fd);
    }
  }

  void handle_client(int client_fd) {
    char hdr[4];
    if (!read_all(client_fd, hdr, sizeof(hdr))) return;
    const uint32_t payload_len = read_u32(hdr);

    constexpr uint32_t MAX_MSG = 4096;
    if (payload_len == 0 || payload_len > MAX_MSG) return;

    std::string payload(payload_len, '\0');
    if (!read_all(client_fd, payload.data(), payload_len)) return;

    apply_update(payload);
  }

  void apply_update(const std::string& payload) {
    if (payload.empty()) return;
    const char* p   = payload.data();
    const char* end = p + payload.size();

    auto remaining = [&]() -> size_t {
      return static_cast<size_t>(end - p);
    };

    if (remaining() < 3) return;
    const auto op = static_cast<MapUpdateOp>(read_u8(p++));
    const uint16_t key_len = read_u16(p); p += 2;

    if (remaining() < key_len) return;
    const std::string key(p, key_len);
    p += key_len;

    switch (op) {

      case MapUpdateOp::SET: {
        if (remaining() < 1) return;
        const auto vtype = static_cast<MapValueType>(read_u8(p++));
        BackgroundMapValue value;
        switch (vtype) {
          case MapValueType::STRING: {
            if (remaining() < 4) return;
            const uint32_t slen = read_u32(p); p += 4;
            if (remaining() < slen) return;
            value = std::string(p, slen);
            break;
          }
          case MapValueType::INT64: {
            if (remaining() < 8) return;
            value = static_cast<long long int>(read_raw<int64_t>(p));
            break;
          }
          case MapValueType::DOUBLE: {
            if (remaining() < 8) return;
            value = read_raw<double>(p);
            break;
          }
          case MapValueType::BOOL: {
            if (remaining() < 1) return;
            value = (read_u8(p) != 0);
            break;
          }
          default: return;
        }
        {
          std::lock_guard lk(*mtx_);
          map_->insert_or_assign(key, std::move(value));
        }
        break;
      }

      case MapUpdateOp::ERASE: {
        std::lock_guard lk(*mtx_);
        map_->erase(key);
        break;
      }

      case MapUpdateOp::INCREMENT: {
        if (remaining() < 1) return;
        const auto ntype = static_cast<MapValueType>(read_u8(p++));

        BackgroundMapValue delta;
        if (ntype == MapValueType::INT64) {
          if (remaining() < 8) return;
          delta = static_cast<long long int>(read_raw<int64_t>(p));
        } else if (ntype == MapValueType::DOUBLE) {
          if (remaining() < 8) return;
          delta = read_raw<double>(p);
        } else {
          return;
        }

        {
          std::lock_guard lk(*mtx_);
          auto it = map_->find(key);
          if (it == map_->end()) {
            map_->insert_or_assign(key, delta);
          } else {
            auto& val = it->second;
            std::visit([&val](auto&& d) {
              using D = std::decay_t<decltype(d)>;
              std::visit([&val, &d](auto&& v) {
                using V = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<V, long long int> && std::is_same_v<D, long long int>) {
                  val = v + d;
                } else if constexpr (std::is_same_v<V, double> && std::is_same_v<D, double>) {
                  val = v + d;
                } else if constexpr (std::is_same_v<V, double> && std::is_same_v<D, long long int>) {
                  val = v + static_cast<double>(d);
                } else if constexpr (std::is_same_v<V, long long int> && std::is_same_v<D, double>) {
                  val = static_cast<double>(v) + d;
                }
              }, val);
            }, delta);
          }
        }
        break;
      }

      default: break;
    }
  }

  std::string socket_path_;
  BackgroundMap* map_ = nullptr;
  std::mutex* mtx_ = nullptr;
  const DoutPrefixProvider* dpp_ = nullptr;
  int listen_fd_ = -1;
  std::atomic<bool> running_{false};
  std::thread thread_;
};

} // namespace rgw::lua
