#include "order_book.hpp"
#include "protocol.hpp"
#include "engine_queue.hpp"
#include "market_data.hpp"
#include "net_util.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <cmath>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <unistd.h>

static std::atomic<bool> g_running{true};
static int g_server_fd = -1;
static std::atomic<int64_t> g_order_id{1};

static void handle_sigint(int) {
    g_running = false;
    if (g_server_fd >= 0) { close(g_server_fd); g_server_fd = -1; }
    std::cerr << "\n[Signal] SIGINT received. Shutting down server...\n";
}


// Engine loop: handle NEW / CANCEL / MODIFY; send TCP reply lines and UDP market‑data lines.
static void engine_loop(OrderQueue& q, std::atomic<bool>& running,
                        int64_t tick_factor, const MarketDataPublisher* md) {
    OrderBook book;

    auto fmt_price = [tick_factor](int64_t ticks) -> std::string {
        std::ostringstream oss;
        oss.setf(std::ios::fixed); oss.precision(2);
        oss << (static_cast<double>(ticks) / static_cast<double>(tick_factor));
        return oss.str();
    };

    while (running) {
        auto mo = q.pop();
        if (!mo.has_value()) break;
        const OrderMsg& m = *mo;

        std::vector<std::string> lines;
        switch (m.type) {
            case MsgType::New:
                lines = book.processOrder(m.side, m.qty, m.price_ticks, m.order_id, fmt_price);
                break;
            case MsgType::Cancel:
                lines = book.cancel(m.order_id, fmt_price);
                break;
            case MsgType::Modify:
                // For modify, we re-use m.qty / m.price_ticks as new params, and generate new id now.
                // Engine assigns new id so replacements are unique in the book history.
                lines = book.replace(m.order_id, m.qty, m.price_ticks,
                                     /*new_id*/ m.order_id, fmt_price);
                break;
        }

        if (lines.empty()) continue;

        std::ostringstream out;
        for (auto& l : lines) {
            out << l << "\n";
            if (md && md->enabled()) md->sendLine(l);
        }
        const std::string payload = out.str();
        (void)send_all(m.client_fd, payload.c_str(), payload.size());
    }
}

static void enqueue_or_error(OrderQueue& q, const OrderMsg& msg, int client_fd) {
    if (!q.push(msg)) {
        const char* err = "ERROR Engine offline\n";
        (void)send_all(client_fd, err, std::strlen(err));
    }
}

static void serve_client(int client_fd, OrderQueue& q, int64_t tick_factor) {
    std::string line;
    while (g_running) {
        if (!recv_line(client_fd, line)) { std::cout << "Client disconnected.\n"; break; }
        if (line.empty()) { std::cout << "Empty line -> close.\n"; break; }

        // ACK timestamp (for client RTT)
        auto now = std::chrono::high_resolution_clock::now();
        long long ts_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
        {
            std::ostringstream wire; wire << "ACK " << ts_us << "\n";
            const std::string w = wire.str();
            (void)send_all(client_fd, w.c_str(), w.size());
        }

        if (line == "QUIT") {
            std::ostringstream bye; bye << "BYE\n";
            const std::string payload = bye.str();
            (void)send_all(client_fd, payload.c_str(), payload.size());
            std::cout << "Client requested QUIT.\n";
            break;
        }

        // Parse commands:
        // NEW BUY|SELL <qty> @ <price>
        // CXL <order_id>
        // MOD <order_id> <new_qty> @ <new_price>
        std::istringstream iss(line);
        std::string cmd; iss >> cmd;
        if (cmd == "NEW") {
            std::string sideStr; int qty=0; char at=0; double price=0.0;
            iss >> sideStr >> qty >> at >> price;
            if ((sideStr != "BUY" && sideStr != "SELL") || at != '@' || qty <= 0 || price <= 0.0) {
                const char* err = "ERROR Invalid NEW. Expected: NEW BUY|SELL <qty> @ <price>\n";
                (void)send_all(client_fd, err, std::strlen(err));
                continue;
            }
            int64_t price_ticks = static_cast<int64_t>(std::llround(price * static_cast<double>(tick_factor)));
            OrderMsg msg;
            msg.type       = MsgType::New;
            msg.side       = (sideStr == "BUY" ? Side::Buy : Side::Sell);
            msg.qty        = qty;
            msg.price_ticks= price_ticks;
            msg.order_id   = g_order_id.fetch_add(1, std::memory_order_relaxed);
            msg.client_fd  = client_fd;
            enqueue_or_error(q, msg, client_fd);
        } else if (cmd == "CXL") {
            int64_t id=0; iss >> id;
            if (id <= 0) {
                const char* err = "ERROR Invalid CXL. Expected: CXL <order_id>\n";
                (void)send_all(client_fd, err, std::strlen(err));
                continue;
            }
            OrderMsg msg; msg.type = MsgType::Cancel; msg.order_id = id; msg.client_fd = client_fd;
            enqueue_or_error(q, msg, client_fd);
        } else if (cmd == "MOD") {
            int64_t id=0; int new_qty=0; char at=0; double new_px=0.0;
            iss >> id >> new_qty >> at >> new_px;
            if (id <= 0 || new_qty <= 0 || at != '@' || new_px <= 0.0) {
                const char* err = "ERROR Invalid MOD. Expected: MOD <order_id> <new_qty> @ <new_price>\n";
                (void)send_all(client_fd, err, std::strlen(err));
                continue;
            }
            int64_t price_ticks = static_cast<int64_t>(std::llround(new_px * static_cast<double>(tick_factor)));
            OrderMsg msg; msg.type = MsgType::Modify;
            msg.order_id = id; msg.qty = new_qty; msg.price_ticks = price_ticks; msg.client_fd = client_fd;
            enqueue_or_error(q, msg, client_fd);
        } else {
            const char* err = "ERROR Unknown command. Use NEW/CXL/MOD/QUIT.\n";
            (void)send_all(client_fd, err, std::strlen(err));
        }
    }

    close(client_fd);
}

int main(int argc, char** argv) {
    std::signal(SIGINT,  handle_sigint);
    std::signal(SIGPIPE, SIG_IGN);

    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    const int64_t TICK_FACTOR = 100;     // £0.01 ticks
    std::string md_host = "127.0.0.1";   // UDP publish target
    uint16_t    md_port = 9001;
    bool        md_on   = true;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--no-md") md_on = false;
        else if (a == "--md-host" && i+1 < argc) md_host = argv[++i];
        else if (a == "--md-port" && i+1 < argc) md_port = static_cast<uint16_t>(std::atoi(argv[++i]));
    }

    g_server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_server_fd < 0) { perror("socket"); return 1; }
    int yes = 1; setsockopt(g_server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{}; addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);
    if (bind(g_server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); close(g_server_fd); return 1; }
    if (listen(g_server_fd, 64) < 0) { perror("listen"); close(g_server_fd); return 1; }

    std::cout << "Exchange waiting for connections... (Ctrl-C to quit)\n";
    if (md_on) std::cout << "Publishing market-data UDP to " << md_host << ":" << md_port << "\n";

    MarketDataPublisher md(md_host, md_port, md_on);
    OrderQueue queue(4096);
    std::atomic<bool> engine_running{true};
    std::thread engine_thr(engine_loop, std::ref(queue), std::ref(engine_running), TICK_FACTOR, &md);

    while (g_running) {
        socklen_t len = sizeof(addr);
        int client_fd = accept(g_server_fd, (sockaddr*)&addr, &len);
        if (client_fd < 0) { if (!g_running) break; perror("accept"); continue; }
        std::cout << "Client connected!\n";
        std::thread(serve_client, client_fd, std::ref(queue), TICK_FACTOR).detach();
    }

    if (g_server_fd >= 0) close(g_server_fd);
    engine_running = false;
    queue.stop();
    if (engine_thr.joinable()) engine_thr.join();

    std::cout << "Server shut down.\n";
    return 0;
}