#pragma once
#include <cerrno>
#include <cstddef>
#include <string>
#include <sys/socket.h>

// Send exactly len bytes, looping over short writes.
// Returns true on success, false if the peer connection failed.
inline bool send_all(int fd, const void* buf, size_t len) {
    const char* p = static_cast<const char*>(buf);
    size_t left = len;
    while (left > 0) {
#ifdef MSG_NOSIGNAL
        ssize_t n = send(fd, p, left, MSG_NOSIGNAL);
#else
        ssize_t n = send(fd, p, left, 0);
#endif
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += n;
        left -= static_cast<size_t>(n);
    }
    return true;
}

// Read one '\n'-terminated line byte by byte.
// Returns true on success, false on EOF/error.
// Retries on EINTR so a caught signal does not drop the client.
inline bool recv_line(int fd, std::string& line) {
    line.clear();
    char ch = 0;
    while (true) {
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n == 0) return false; // EOF
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (ch == '\n') break;
        line.push_back(ch);
        if (line.size() > 8192) { line.clear(); return false; }
    }
    return true;
}
