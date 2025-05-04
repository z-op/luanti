#include "postgresql_lru.h"
#include "util/numeric.h"
#include "debug.h"
#include <fmt/format.h>

BaseDatabasePostgreSQL::BaseDatabasePostgreSQL(const std::string& connect_string,
                                             const std::string& db_type,
                                             size_t cache_size)
    : db_type_(db_type),
      conn_pool_(std::make_unique<ConnectionPool>(connect_string)),
      cache_(cache_size) {
}

BaseDatabasePostgreSQL::~BaseDatabasePostgreSQL() {
    shutdown();
}

void BaseDatabasePostgreSQL::initialize() {
    // Start async worker thread
    running_ = true;
    worker_thread_ = std::thread(&BaseDatabasePostgreSQL::workerMain, this);

    // Initialize database connection
    auto conn = acquireConnection();
    setupConnection(conn.get());
    validateSchema();
    initStatements();
}

void BaseDatabasePostgreSQL::shutdown() {
    running_ = false;
    queue_cv_.notify_all();

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    if (in_transaction_) {
        rollbackTransaction();
    }
}

// Transaction management
void BaseDatabasePostgreSQL::beginTransaction() {
    auto conn = acquireConnection();
    PGresult* res = PQexec(conn.get(), "BEGIN");
    checkResult(res, PGRES_COMMAND_OK);
    in_transaction_ = true;
}

void BaseDatabasePostgreSQL::commitTransaction() {
    auto conn = acquireConnection();
    PGresult* res = PQexec(conn.get(), "COMMIT");
    checkResult(res, PGRES_COMMAND_OK);
    in_transaction_ = false;
}

void BaseDatabasePostgreSQL::rollbackTransaction() {
    auto conn = acquireConnection();
    PGresult* res = PQexec(conn.get(), "ROLLBACK");
    checkResult(res, PGRES_COMMAND_OK);
    in_transaction_ = false;
}

// Async operations
template<typename F>
void BaseDatabasePostgreSQL::enqueueAsync(F&& task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        async_queue_.emplace(std::forward<F>(task));
    }
    queue_cv_.notify_one();
}

// Cache operations
bool BaseDatabasePostgreSQL::getFromCache(const std::string& category,
                                        const std::string& key,
                                        std::string& value) {
    return cache_.get(category, key, value);
}

void BaseDatabasePostgreSQL::putToCache(const std::string& category,
                                      const std::string& key,
                                      const std::string& value,
                                      int ttl) {
    cache_.put(category, key, value, ttl);
}

void BaseDatabasePostgreSQL::removeFromCache(const std::string& category,
                                           const std::string& key) {
    cache_.remove(category, key);
}

// Connection management
std::unique_ptr<PGconn, BaseDatabasePostgreSQL::PGConnectionDeleter>
BaseDatabasePostgreSQL::acquireConnection() {
    return conn_pool_->getConnection();
}

void BaseDatabasePostgreSQL::releaseConnection(
    std::unique_ptr<PGconn, PGConnectionDeleter> conn) {
    conn_pool_->returnConnection(std::move(conn));
}

// Query execution
PGresult* BaseDatabasePostgreSQL::executeQuery(const std::string& query) {
    auto conn = acquireConnection();
    PGresult* res = PQexec(conn.get(), query.c_str());
    checkResult(res, PGRES_COMMAND_OK);
    return res;
}

PGresult* BaseDatabasePostgreSQL::executePrepared(const std::string& stmt_name,
                                                const std::vector<const char*>& params,
                                                const std::vector<int>& param_lengths,
                                                const std::vector<int>& param_formats) {
    auto conn = acquireConnection();
    PGresult* res = PQexecPrepared(
        conn.get(),
        stmt_name.c_str(),
        params.size(),
        params.data(),
        param_lengths.data(),
        param_formats.data(),
        1 // Binary results
    );
    checkResult(res, PGRES_TUPLES_OK);
    return res;
}

// Utility functions
std::string BaseDatabasePostgreSQL::serializePosition(const v3s16& pos) {
    return fmt::format("{},{},{}", pos.X, pos.Y, pos.Z);
}

v3s16 BaseDatabasePostgreSQL::deserializePosition(const std::string& data) {
    std::istringstream iss(data);
    char delimiter;
    v3s16 pos;
    iss >> pos.X >> delimiter >> pos.Y >> delimiter >> pos.Z;
    return pos;
}

// Connection pool implementation
BaseDatabasePostgreSQL::ConnectionPool::ConnectionPool(
    const std::string& conn_str, size_t pool_size)
    : conn_str_(conn_str), pool_size_(pool_size) {
    for (size_t i = 0; i < pool_size_; ++i) {
        connections_.emplace(PQconnectdb(conn_str_.c_str()));
    }
}

std::unique_ptr<PGconn, BaseDatabasePostgreSQL::PGConnectionDeleter>
BaseDatabasePostgreSQL::ConnectionPool::getConnection() {
    std::unique_lock<std::mutex> lock(pool_mutex_);
    if (connections_.empty()) {
        return std::unique_ptr<PGconn, PGConnectionDeleter>(
            PQconnectdb(conn_str_.c_str()));
    }

    auto conn = std::move(connections_.front());
    connections_.pop();
    return conn;
}

void BaseDatabasePostgreSQL::ConnectionPool::returnConnection(
    std::unique_ptr<PGconn, PGConnectionDeleter> conn) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    if (connections_.size() < pool_size_) {
        connections_.push(std::move(conn));
    }
}

// Private methods
void BaseDatabasePostgreSQL::workerMain() {
    while (running_) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this]{
            return !async_queue_.empty() || !running_;
        });

        while (!async_queue_.empty()) {
            auto task = std::move(async_queue_.front());
            async_queue_.pop();
            lock.unlock();

            try {
                task();
            } catch (const DatabaseException& e) {
                errorstream << "Async task failed: " << e.what();
            }

            lock.lock();
        }
    }
}

void BaseDatabasePostgreSQL::checkResult(PGresult* res, ExecStatusType expected_status) {
    const ExecStatusType actual_status = PQresultStatus(res);
    if (actual_status != expected_status) {
        const std::string error_msg = PQresultErrorMessage(res);
        PQclear(res);
        throw DatabaseException(fmt::format(
            "Query failed (Expected: {}, Actual: {}): {}",
            PQresStatus(expected_status),
            PQresStatus(actual_status),
            error_msg
        ));
    }
    PQclear(res);
}

void BaseDatabasePostgreSQL::reconnectIfNeeded() {
    auto conn = acquireConnection();
    if (PQstatus(conn.get()) != CONNECTION_OK) {
        conn.reset(PQconnectdb(conn_pool_->conn_str_.c_str()));
        if (PQstatus(conn.get()) != CONNECTION_OK) {
            throw DatabaseException("Reconnection failed: " +
                std::string(PQerrorMessage(conn.get())));
        }
        setupConnection(conn.get());
    }
}

class MapDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    MapDatabasePostgreSQL(const std::string& connect_string)
        : BaseDatabasePostgreSQL(connect_string, "map", 500000) {}

    bool saveBlock(const v3s16& pos, const std::string& data) {
        const std::string key = serializePosition(pos);

        // Асинхронное кэширование
        enqueueAsync([this, key, data] {
            putToCache("blocks", key, data);
        });

        // Пакетная подготовка данных
        const int16_t coords[3] = {pos.X, pos.Y, pos.Z};
        const std::vector<const char*> params = {
            reinterpret_cast<const char*>(coords),
            data.data()
        };
        const std::vector<int> lengths = {
            sizeof(coords),
            static_cast<int>(data.size())
        };

        try {
            executePrepared("save_block", params, lengths, {1,1});
            return true;
        } catch (const DatabaseException& e) {
            errorstream << "Save block failed: " << e.what();
            return false;
        }
    }

    bool loadBlock(const v3s16& pos, std::string& data) {
        const std::string key = serializePosition(pos);
        if (getFromCache("blocks", key, data)) {
            return !data.empty();
        }

        const int16_t coords[3] = {pos.X, pos.Y, pos.Z};
        const std::vector<const char*> params = {
            reinterpret_cast<const char*>(coords)
        };

        PGresult* res = executePrepared("load_block", params, {sizeof(coords)}, {1});
        if (PQntuples(res) > 0) {
            data.assign(PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
            putToCache("blocks", key, data);
            return true;
        }
        return false;
    }

protected:
    void initStatements() override {
        prepareStatement("save_block",
            "INSERT INTO blocks (pos, data) VALUES ($1::smallint[], $2::bytea) "
            "ON CONFLICT (pos) DO UPDATE SET data = EXCLUDED.data");

        prepareStatement("load_block",
            "SELECT data FROM blocks WHERE pos = $1::smallint[]");
    }

    void setupConnection(PGconn* conn) override {
        PQexec(conn, "SET bytea_output = 'escape'");
    }

    void validateSchema() override {
        // Проверка существования таблиц и индексов
    }
};

class PlayerDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    struct PlayerData {
        v3f position;
        float pitch;
        float yaw;
        std::unordered_map<std::string, std::string> metadata;
    };

    PlayerDatabasePostgreSQL(const std::string& connect_string)
        : BaseDatabasePostgreSQL(connect_string, "player") {}

    void savePlayer(const std::string& name, const PlayerData& data) {
        beginTransaction();
        try {
            // Сохранение основной информации
            savePlayerMainData(name, data);

            // Сохранение метаданных пакетом
            saveMetadataBatch(name, data.metadata);

            commitTransaction();
        } catch (...) {
            rollbackTransaction();
            throw;
        }
    }

private:
    void savePlayerMainData(const std::string& name, const PlayerData& data) {
        const std::vector<const char*> params = {
            name.c_str(),
            reinterpret_cast<const char*>(&data.position),
            reinterpret_cast<const char*>(&data.pitch),
            reinterpret_cast<const char*>(&data.yaw)
        };

        executePrepared("save_player_main", params, {}, {0,1,1,1});
    }

    void saveMetadataBatch(const std::string& name,
                          const std::unordered_map<std::string, std::string>& meta) {
        std::vector<const char*> names, keys, values;
        for (const auto& [k, v] : meta) {
            names.push_back(name.c_str());
            keys.push_back(k.c_str());
            values.push_back(v.c_str());
        }

        const std::vector<const char*> params = {
            reinterpret_cast<const char*>(names.data()),
            reinterpret_cast<const char*>(keys.data()),
            reinterpret_cast<const char*>(values.data())
        };

        executePrepared("save_metadata_batch", params, {}, {1,1,1});
    }

protected:
    void initStatements() override {
        prepareStatement("save_player_main",
            "INSERT INTO players (name, position, pitch, yaw) "
            "VALUES ($1, $2::bytea, $3::float4, $4::float4) "
            "ON CONFLICT (name) DO UPDATE SET "
            "position = EXCLUDED.position, "
            "pitch = EXCLUDED.pitch, "
            "yaw = EXCLUDED.yaw");

        prepareStatement("save_metadata_batch",
            "INSERT INTO player_metadata (name, key, value) "
            "SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::text[]) "
            "ON CONFLICT (name, key) DO UPDATE SET value = EXCLUDED.value");
    }
};

class AuthDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    struct AuthEntry {
        std::string name;
        std::string salt;
        std::string hash;
        std::vector<std::string> privileges;
    };

    AuthDatabasePostgreSQL(const std::string& connect_string)
        : BaseDatabasePostgreSQL(connect_string, "auth") {}

    bool createAuth(const AuthEntry& entry) {
        beginTransaction();
        try {
            // Вставка основной записи
            const std::vector<const char*> main_params = {
                entry.name.c_str(),
                entry.salt.c_str(),
                entry.hash.c_str()
            };
            PGresult* res = executePrepared("create_auth", main_params, {}, {0,0,0});

            // Получение ID
            const int id = PQgetvalue(res, 0, 0);

            // Вставка привилегий
            savePrivilegesBatch(id, entry.privileges);

            commitTransaction();
            return true;
        } catch (...) {
            rollbackTransaction();
            throw;
        }
    }

private:
    void savePrivilegesBatch(int user_id,
                            const std::vector<std::string>& privileges) {
        std::vector<int> ids(privileges.size(), user_id);
        std::vector<const char*> privs;
        for (const auto& p : privileges) privs.push_back(p.c_str());

        const std::vector<const char*> params = {
            reinterpret_cast<const char*>(ids.data()),
            reinterpret_cast<const char*>(privs.data())
        };

        executePrepared("save_privs_batch", params, {}, {1,1});
    }

protected:
    void initStatements() override {
        prepareStatement("create_auth",
            "INSERT INTO auth (name, salt, hash) "
            "VALUES ($1, $2, $3) RETURNING id");

        prepareStatement("save_privs_batch",
            "INSERT INTO privileges (user_id, privilege) "
            "SELECT unnest($1::int[]), unnest($2::text[])");
    }
};

class ModStorageDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    ModStorageDatabasePostgreSQL(const std::string& connect_string)
        : BaseDatabasePostgreSQL(connect_string, "mod_storage", 1000000) {}

    void setModEntry(const std::string& modname,
                    const std::string& key,
                    const std::string& value) {
        // Асинхронная запись с кэшированием
        enqueueAsync([=] {
            const std::vector<const char*> params = {
                modname.c_str(),
                key.c_str(),
                value.c_str()
            };
            executePrepared("upsert_mod_entry", params, {}, {0,0,0});
            putToCache(modname, key, value);
        });
    }

protected:
    void initStatements() override {
        prepareStatement("upsert_mod_entry",
            "INSERT INTO mod_storage (modname, key, value) "
            "VALUES ($1, $2, $3) "
            "ON CONFLICT (modname, key) DO UPDATE SET "
            "value = EXCLUDED.value");
    }

    void validateSchema() override {
        // Автоматическое создание таблицы при необходимости
        executeQuery(
            "CREATE TABLE IF NOT EXISTS mod_storage ("
            "modname TEXT NOT NULL, "
            "key TEXT NOT NULL, "
            "value TEXT NOT NULL, "
            "PRIMARY KEY (modname, key))");
    }
};

// Основные операции с блоками
bool saveBlock(const v3s16& pos, const std::string& data);
bool loadBlock(const v3s16& pos, std::string& data);
bool deleteBlock(const v3s16& pos);
void listAllBlocks(std::vector<v3s16>& output);

// Массовые операции
void saveBlocksBatch(const std::unordered_map<v3s16, std::string>& blocks);
void loadBlocksArea(const v3s16& min_pos, const v3s16& max_pos,
                   std::unordered_map<v3s16, std::string>& output);

// Вспомогательные методы
bool blockExists(const v3s16& pos);
void optimizeDatabase();
void repairDatabase();

class MapDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    MapDatabasePostgreSQL(const std::string& connect_string)
        : BaseDatabasePostgreSQL(connect_string, "map") {}

protected:
    void createDatabase() override {
        executeQuery(R"(
            CREATE TABLE IF NOT EXISTS blocks (
                pos_x SMALLINT NOT NULL,
                pos_y SMALLINT NOT NULL,
                pos_z SMALLINT NOT NULL,
                data BYTEA NOT NULL,
                modified TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (pos_x, pos_y, pos_z)
            );

            CREATE INDEX IF NOT EXISTS blocks_modified_idx
            ON blocks (modified);
        )");
    }

    void initPreparedStatements() override {
        prepareStatement("save_block",
            "INSERT INTO blocks (pos_x, pos_y, pos_z, data) "
            "VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (pos_x, pos_y, pos_z) "
            "DO UPDATE SET data = EXCLUDED.data, modified = NOW()");

        prepareStatement("load_block",
            "SELECT data FROM blocks "
            "WHERE pos_x = $1 AND pos_y = $2 AND pos_z = $3");

        prepareStatement("delete_block",
            "DELETE FROM blocks "
            "WHERE pos_x = $1 AND pos_y = $2 AND pos_z = $3");
    }

    void checkSchemaVersion() override {
        if (!columnExists("blocks", "modified")) {
            executeQuery("ALTER TABLE blocks ADD COLUMN modified TIMESTAMP");
            executeQuery("UPDATE blocks SET modified = NOW()");
        }
    }
};

class PlayerDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    PlayerDatabasePostgreSQL(const std::string& connect_string)
        : BaseDatabasePostgreSQL(connect_string, "player") {}

protected:
    void createDatabase() override {
        executeQuery(R"(
            CREATE TABLE IF NOT EXISTS players (
                name TEXT PRIMARY KEY,
                pos_x REAL NOT NULL,
                pos_y REAL NOT NULL,
                pos_z REAL NOT NULL,
                inventory JSONB NOT NULL,
                created TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS player_meta (
                player TEXT REFERENCES players(name),
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (player, key)
            );
        )");
    }

    void initPreparedStatements() override {
        prepareStatement("upsert_player",
            "INSERT INTO players (name, pos_x, pos_y, pos_z, inventory) "
            "VALUES ($1, $2, $3, $4, $5) "
            "ON CONFLICT (name) DO UPDATE SET "
            "pos_x = EXCLUDED.pos_x, "
            "pos_y = EXCLUDED.pos_y, "
            "pos_z = EXCLUDED.pos_z, "
            "inventory = EXCLUDED.inventory");

        prepareStatement("save_meta",
            "INSERT INTO player_meta (player, key, value) "
            "VALUES ($1, $2, $3) "
            "ON CONFLICT (player, key) DO UPDATE SET value = EXCLUDED.value");
    }

    void checkSchemaVersion() override {
        if (!tableExists("player_meta")) {
            executeQuery(R"(
                CREATE TABLE player_meta (
                    player TEXT REFERENCES players(name),
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (player, key)
                )
            )");
        }
    }
};

class AuthDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    AuthDatabasePostgreSQL(const std::string& connect_string)
        : BaseDatabasePostgreSQL(connect_string, "auth") {}

protected:
    void createDatabase() override {
        executeQuery(R"(
            CREATE TABLE IF NOT EXISTS accounts (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                last_login TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS privileges (
                account_id INTEGER REFERENCES accounts(id),
                privilege TEXT NOT NULL,
                PRIMARY KEY (account_id, privilege)
            );
        )");
    }

    void initPreparedStatements() override {
        prepareStatement("create_account",
            "INSERT INTO accounts (name, password_hash) "
            "VALUES ($1, $2) RETURNING id");

        prepareStatement("grant_privilege",
            "INSERT INTO privileges (account_id, privilege) "
            "VALUES ($1, $2) ON CONFLICT DO NOTHING");
    }

    void checkSchemaVersion() override {
        if (!columnExists("accounts", "last_login")) {
            executeQuery("ALTER TABLE accounts ADD COLUMN last_login TIMESTAMP");
        }
    }
};

class ModStorageDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    ModStorageDatabasePostgreSQL(const std::string& connect_string)
        : BaseDatabasePostgreSQL(connect_string, "mod_storage") {}

protected:
    void createDatabase() override {
        executeQuery(R"(
            CREATE TABLE IF NOT EXISTS mod_data (
                modname TEXT NOT NULL,
                key TEXT NOT NULL,
                value BYTEA NOT NULL,
                expires TIMESTAMP,
                PRIMARY KEY (modname, key)
            );

            CREATE INDEX IF NOT EXISTS mod_data_expires_idx
            ON mod_data (expires);
        )");
    }

    void initPreparedStatements() override {
        prepareStatement("upsert_mod_data",
            "INSERT INTO mod_data (modname, key, value, expires) "
            "VALUES ($1, $2, $3, NOW() + INTERVAL '1 day') "
            "ON CONFLICT (modname, key) DO UPDATE SET "
            "value = EXCLUDED.value, expires = EXCLUDED.expires");

        prepareStatement("cleanup_expired",
            "DELETE FROM mod_data WHERE expires < NOW()");
    }

    void checkSchemaVersion() override {
        if (!columnExists("mod_data", "expires")) {
            executeQuery("ALTER TABLE mod_data ADD COLUMN expires TIMESTAMP");
            executeQuery("CREATE INDEX mod_data_expires_idx ON mod_data (expires)");
        }
    }
};

void checkResult(PGresult* res, ExecStatusType expected) {
    ExecStatusType status = PQresultStatus(res);
    if (status != expected) {
        std::string err = PQresultErrorMessage(res);
        PQclear(res);
        throw DatabaseException(fmt::format(
            "[{}] Ошибка запроса (ожидалось: {}, получено: {}): {}",
            m_type, PQresStatus(expected), PQresStatus(status), err
        ));
    }
    PQclear(res);
}

PGconn* getConnectionWithRetry() {
    auto conn = acquireConnection();
    if (PQstatus(conn.get()) != CONNECTION_OK) {
        conn.reset(PQconnectdb(m_connect_str.c_str()));
        if (PQstatus(conn.get()) != CONNECTION_OK) {
            throw DatabaseException("Не удалось восстановить соединение");
        }
        setupConnection(conn.get()); // Переинициализация prepared statements
    }
    return conn.release();
}

TEST(MapDatabaseTest, BlockSaveLoad) {
    MapDatabasePostgreSQL db("test_conn_str");
    v3s16 pos(10, 20, 30);
    std::string test_data = "test_block_data";

    EXPECT_TRUE(db.saveBlock(pos, test_data));

    std::string loaded_data;
    EXPECT_TRUE(db.loadBlock(pos, loaded_data));
    EXPECT_EQ(test_data, loaded_data);
}

void PlayerDatabasePostgreSQL::serializeInventory(const Inventory& inv) {
    std::ostringstream oss;
    inv.serialize(oss);
    m_current_inventory = oss.str();
}

void PlayerDatabasePostgreSQL::deserializeInventory(Inventory& inv) {
    std::istringstream iss(m_current_inventory);
    inv.deSerialize(iss);
}

std::string serializeNodePosition(const v3s16& pos) {
    return fmt::format("({},{},{})", pos.X, pos.Y, pos.Z);
}

void validatePlayerName(const std::string& name) {
    if (name.empty() || name.size() > 32) {
        throw std::invalid_argument("Недопустимое имя игрока");
    }
    if (name.find_first_of(";'\"") != std::string::npos) {
        throw std::invalid_argument("Запрещенные символы в имени");
    }
}

std::string hashPassword(const std::string& password) {
    return picosha2::hash256_hex_string(password + m_salt);
}

class DatabaseLogger : public Logger {
public:
    void log(LogLevel level, const std::string& message) override {
        std::string prefix = "[PostgreSQL] ";
        switch(level) {
            case LL_ERROR: errorstream << prefix << message; break;
            case LL_WARNING: warningstream << prefix << message; break;
            default: infostream << prefix << message;
        }
    }
};

struct DatabaseMetrics {
    std::atomic<uint64_t> query_count{0};
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> transaction_retries{0};
};

void loadDatabaseConfig() {
    Settings& settings = get_settings();
    m_connect_str = settings.get("pgsql_connection");
    m_cache_size = settings.getU64("pgsql_cache_size", 100000);
}

class ModPostgreSQL : public Mod {
public:
    void on_load() override {
        m_db = std::make_unique<ModStorageDatabasePostgreSQL>(
            get_setting("pgsql_connection")
        );
        m_db->initialize();
    }

    void on_player_join(Player* player) override {
        try {
            PlayerData data;
            if (m_db->loadPlayer(player->getName(), data)) {
                player->setPosition(data.position);
            }
        } catch (const DatabaseException& e) {
            errorstream << "Ошибка загрузки данных игрока: " << e.what();
        }
    }
};

void MapDatabasePostgreSQL::saveBlock(const v3s16& pos, const std::string& data) {
    const std::vector<uint8_t> binary_data(data.begin(), data.end());

    executePrepared("save_block", {
        pos.X, pos.Y, pos.Z,
        pqxx::binary_cast(binary_data) // Специальная обработка BLOB
    });
}
