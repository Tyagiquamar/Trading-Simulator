#include "net_util.hpp"

#include "gtest/gtest.h"

#include <csignal>
#include <cstring>
#include <pthread.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

// Minimal handler so SIGUSR1 interrupts recv() with EINTR instead of killing us.
void noop_handler(int) {}

void send_sigusr1_to_main(void* main_thread) {
    for (int i = 0; i < 3; ++i) {
        usleep(50 * 1000);
        pthread_kill(*static_cast<pthread_t*>(main_thread), SIGUSR1);
    }
    usleep(50 * 1000);
}

} // namespace

// Old implementation sent once and discarded the result: with a full socket
// buffer the payload was silently truncated. send_all must loop over short
// writes until every byte is queued.
TEST(NetFraming, SendAllCompletesShortWrites) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    int sndbuf = 4096;
    ASSERT_EQ(setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf)), 0);

    const size_t payload_size = 1u << 20; // 1 MiB, far above the send buffer
    std::vector<char> payload(payload_size);
    for (size_t i = 0; i < payload_size; ++i) {
        payload[i] = static_cast<char>(i & 0xFF);
    }

    std::vector<char> received(payload_size, 0);
    size_t got = 0;

    // Reader waits first so the sender must hit short/blocking writes.
    std::thread reader([&] {
        usleep(100 * 1000);
        while (got < payload_size) {
            ssize_t n = recv(sv[1], received.data() + got, payload_size - got, 0);
            if (n <= 0) break;
            got += static_cast<size_t>(n);
        }
    });

    EXPECT_TRUE(send_all(sv[0], payload.data(), payload_size));

    reader.join();
    EXPECT_EQ(got, payload_size);
    EXPECT_EQ(std::memcmp(received.data(), payload.data(), payload_size), 0);

    close(sv[0]);
    close(sv[1]);
}

// Old implementation treated recv() == -1 as fatal, so a caught signal
// dropped the client mid-line. recv_line must retry on EINTR.
TEST(NetFraming, RecvLineRetriesAfterEINTR) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    struct sigaction sa{};
    sa.sa_handler = noop_handler; // deliberately no SA_RESTART
    ASSERT_EQ(sigaction(SIGUSR1, &sa, nullptr), 0);

    pthread_t main_thread = pthread_self();
    std::thread signaler(send_sigusr1_to_main, &main_thread);

    std::string line;
    // No data yet: first three wake-ups are EINTR, then real bytes arrive.
    std::thread writer([&] {
        usleep(250 * 1000);
        ASSERT_EQ(write(sv[0], "hello\n", 6), 6);
    });

    EXPECT_TRUE(recv_line(sv[1], line));
    EXPECT_EQ(line, "hello");

    signaler.join();
    writer.join();

    close(sv[0]);
    close(sv[1]);
}
