#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include <libpq-fe.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <stdexcept>
#include <string>

class ConnectionPool {
public:
    // Конфигурация пула
    struct Config {
        std::string connectionString;
        int minConnections = 2;
        int maxConnections = 10;
        std::chrono::seconds timeout = std::chrono::seconds(5);
    };

    // Инициализация с конфигурацией
    explicit ConnectionPool(const Config& config);

    // Запрет копирования
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    ~ConnectionPool();

    // Получение подключения с таймаутом
    std::shared_ptr<PGconn> getConnection();

    // Возврат подключения в пул
    void releaseConnection(std::shared_ptr<PGconn> conn);

    // Статистика
    struct Stats {
        int total;
        int idle;
        int inUse;
    };
    Stats getStats() const;

private:
    Config config_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::shared_ptr<PGconn>> connections_;
    int activeConnections_ = 0;

    // Создание нового подключения
    std::shared_ptr<PGconn> createConnection();

    // Валидация подключения
    bool isValid(const PGconn* conn) const;
};

#endif // CONNECTION_POOL_H
