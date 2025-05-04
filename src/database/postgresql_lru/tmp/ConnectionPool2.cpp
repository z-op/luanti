#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include <libpq-fe.h>
#include <memory>
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <stdexcept>

class ConnectionPool {
public:
    struct ConnectionDeleter {
        void operator()(PGconn* conn) const {
            if (conn) PQfinish(conn);
        }
    };

    using ConnectionPtr = std::unique_ptr<PGconn, ConnectionDeleter>;

    ConnectionPool(const std::string& conn_str,
                  size_t min_connections = 2,
                  size_t max_connections = 10,
                  std::chrono::milliseconds timeout = std::chrono::seconds(5))
        : conn_str_(conn_str),
          min_connections_(min_connections),
          max_connections_(max_connections),
          timeout_(timeout)
    {
        initializePool();
    }

    ConnectionPtr getConnection() {
        std::unique_lock<std::mutex> lock(mutex_);

        if (!cv_.wait_for(lock, timeout_, [this] {
            return !available_.empty() || (created_ < max_connections_);
        })) {
            throw std::runtime_error("Connection timeout");
        }

        if (available_.empty() && created_ < max_connections_) {
            auto conn = createConnection();
            if (conn) {
                created_++;
                return conn;
            }
        }

        while (!available_.empty()) {
            auto conn = std::move(available_.front());
            available_.pop();

            if (isConnectionValid(conn.get())) {
                return conn;
            }
            created_--;
        }

        throw std::runtime_error("Failed to get valid connection");
    }

    void returnConnection(ConnectionPtr conn) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (isConnectionValid(conn.get()) && available_.size() < max_connections_) {
            available_.push(std::move(conn));
        }
        cv_.notify_one();
    }

    size_t availableCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return available_.size();
    }

    size_t createdCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return created_;
    }

private:
    std::string conn_str_;
    size_t min_connections_;
    size_t max_connections_;
    std::chrono::milliseconds timeout_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<ConnectionPtr> available_;
    size_t created_ = 0;

    void initializePool() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (size_t i = 0; i < min_connections_; ++i) {
            if (auto conn = createConnection()) {
                available_.push(std::move(conn));
                created_++;
            }
        }
    }

    ConnectionPtr createConnection() {
        PGconn* conn = PQconnectdb(conn_str_.c_str());
        if (PQstatus(conn) != CONNECTION_OK) {
            PQfinish(conn);
            throw std::runtime_error("Connection failed: " + std::string(PQerrorMessage(conn)));
        }
        return ConnectionPtr(conn);
    }

    bool isConnectionValid(PGconn* conn) const {
        if (!conn || PQstatus(conn) != CONNECTION_OK) {
            return false;
        }

        PGresult* res = PQexec(conn, "SELECT 1");
        bool valid = (PQresultStatus(res) == PGRES_TUPLES_OK);
        PQclear(res);
        return valid;
    }
};

#endif // CONNECTION_POOL_H
