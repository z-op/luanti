#ifndef BASE_DATABASE_POSTGRESQL_H
#define BASE_DATABASE_POSTGRESQL_H

#include <libpq-fe.h>
#include <memory>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <atomic>
#include <functional>
#include "irrlichttypes.h"
#include "database/database.h"
#include "util/numeric.h"
#include "config.h"

class CacheManager;

class BaseDatabasePostgreSQL : public Database {
public:
    struct DatabaseStatus {
        bool connection_valid;
        size_t active_connections;
        size_t queued_operations;
        uint64_t cache_hits;
        uint64_t cache_misses;
    };

    BaseDatabasePostgreSQL(
        const std::string &connect_string,
        const std::string &type,
        size_t cache_size = 100000,
        size_t connection_pool_size = 5
    );
    virtual ~BaseDatabasePostgreSQL();

    // Main interface
    virtual void connect() override;
    virtual void disconnect() override;
    virtual bool isConnected() const override;

    // Transaction management
    virtual void beginSave() override;
    virtual void endSave() override;
    virtual void rollback() override;

    // Async operations
    template<typename F>
    void enqueueAsyncOperation(F&& func);

    // Cache management
    bool cacheGet(const std::string &category,
                 const std::string &key,
                 std::string &value);
    void cachePut(const std::string &category,
                 const std::string &key,
                 const std::string &value,
                 int ttl = 0);
    void cacheRemove(const std::string &category,
                    const std::string &key);
    void cachePurgeCategory(const std::string &category);

    // Status monitoring
    DatabaseStatus getStatus() const;
    void runMaintenanceTasks();

protected:
    // Connection handling
    struct PGConnectionDeleter {
        void operator()(PGconn* conn) const;
    };
    using PGConnection = std::unique_ptr<PGconn, PGConnectionDeleter>;

    PGConnection acquireConnection();
    void releaseConnection(PGConnection conn);

    // Query execution
    PGresult* executeQuery(const std::string &query);
    PGresult* executePrepared(
        const std::string &stmt_name,
        const std::vector<const char*> &params,
        const std::vector<int> &param_lengths,
        const std::vector<int> &param_formats
    );

    // Schema management
    virtual void createDatabase() = 0;
    virtual void initPreparedStatements() = 0;
    virtual void checkSchemaVersion() = 0;

    // Utility functions
    static std::string serializePosition(const v3s16 &pos);
    static v3s16 deserializePosition(const std::string &data);
    static std::string formatArray(const std::vector<std::string> &elements);

private:
    // Connection pool implementation
    class ConnectionPool {
    public:
        ConnectionPool(const std::string &conn_str, size_t pool_size);
        PGConnection getConnection();
        void returnConnection(PGConnection conn);
        size_t availableConnections() const;
        size_t activeConnections() const;

    private:
        std::string m_conn_str;
        size_t m_pool_size;
        mutable std::mutex m_mutex;
        std::queue<PGConnection> m_available;
        std::vector<PGConnection> m_in_use;
    };

    // Async processing
    void processAsyncQueue();
    void startAsyncThread();
    void stopAsyncThread();

    std::string m_connect_str;
    std::string m_type;
    std::unique_ptr<ConnectionPool> m_conn_pool;
    std::unique_ptr<CacheManager> m_cache;

    // Async processing members
    std::queue<std::function<void()>> m_async_queue;
    mutable std::mutex m_async_mutex;
    std::condition_variable m_async_cv;
    std::atomic<bool> m_async_running{false};
    std::thread m_async_thread;

    // State management
    std::atomic<bool> m_in_transaction{false};
    std::atomic<size_t> m_active_operations{0};
};

// Template implementation
template<typename F>
void BaseDatabasePostgreSQL::enqueueAsyncOperation(F&& func) {
    {
        std::lock_guard<std::mutex> lock(m_async_mutex);
        m_async_queue.emplace(std::forward<F>(func));
    }
    m_async_cv.notify_one();
}

#endif // BASE_DATABASE_POSTGRESQL_H
