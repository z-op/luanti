#include "ConnectionPool.h"
#include <chrono>

ConnectionPool::ConnectionPool(const Config& config) : config_(config) {
    for (int i = 0; i < config_.minConnections; ++i) {
        connections_.push(createConnection());
    }
}

ConnectionPool::~ConnectionPool() {
    std::lock_guard<std::mutex> lock(mutex_);
    while (!connections_.empty()) {
        PQfinish(connections_.front().get());
        connections_.pop();
    }
}

std::shared_ptr<PGconn> ConnectionPool::getConnection() {
    std::unique_lock<std::mutex> lock(mutex_);

    auto waitUntil = std::chrono::steady_clock::now() + config_.timeout;
    while (true) {
        // Возвращаем свободное подключение
        if (!connections_.empty()) {
            auto conn = connections_.front();
            connections_.pop();
            activeConnections_++;
            return conn;
        }

        // Создаем новое, если не превышен лимит
        if (activeConnections_ < config_.maxConnections) {
            activeConnections_++;
            return createConnection();
        }

        // Ожидаем освобождения
        if (cv_.wait_until(lock, waitUntil) == std::cv_status::timeout) {
            throw std::runtime_error("Connection timeout");
        }
    }
}

void ConnectionPool::releaseConnection(std::shared_ptr<PGconn> conn) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (isValid(conn.get())) {
        connections_.push(conn);
    } else {
        PQfinish(conn.get());
    }
    activeConnections_--;
    cv_.notify_one();
}

ConnectionPool::Stats ConnectionPool::getStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return {
        activeConnections_ + static_cast<int>(connections_.size()),
        static_cast<int>(connections_.size()),
        activeConnections_
    };
}

std::shared_ptr<PGconn> ConnectionPool::createConnection() {
    PGconn* conn = PQconnectdb(config_.connectionString.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::string error = PQerrorMessage(conn);
        PQfinish(conn);
        throw std::runtime_error("Connection failed: " + error);
    }
    return std::shared_ptr<PGconn>(conn, [this](PGconn* c) { releaseConnection(std::shared_ptr<PGconn>(c)); });
}

bool ConnectionPool::isValid(const PGconn* conn) const {
    return conn && PQstatus(conn) == CONNECTION_OK &&
           PQpingParams(config_.connectionString.c_str(), nullptr, 0) == PQPING_OK;
}
