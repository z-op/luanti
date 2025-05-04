// Luanti fast asyncronous postgresql connector
// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (C) 2016 Loic Blot <loic.blot@unix-experience.fr>
// Copyright (C) 2025 Evgeniy Pudikov <evgenuel@gmail.com>
// Хорошо, я смотрю на этот код и пытаюсь понять, что здесь происходит.(c) DeepSeek
//todo async bath and LRU for MapDatabasePostgreSQL ModStorageDatabasePostgreSQL
//todo multithreading support
//todo check minetest api compatibility



#include "config.h"
#if USE_POSTGRESQL
#include "database-postgresql_lru.h"
#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "exceptions.h"
#include "settings.h"
#include "remoteplayer.h"
#include "server/player_sao.h"
#include "util/numeric.h" // Для clamp()
#include <cstdlib>
#include <ostream>
#include "debug.h" // Для infostream
#include <fmt/ranges.h>
#include <limits>
#include <array>
#include <sstream>

namespace irr {
	namespace core {
		std::ostream & operator << (std::ostream & os,
			const vector3d < short > & v) {
			return os << "(" << v.X << ", " << v.Y << ", " << v.Z << ")";
		}
	} // namespace core
} // namespace irr
CacheManager::CacheManager(size_t size): max_size(size) {
	if(max_size == 0) {
		throw std::invalid_argument("Cache size must be greater than 0");
	}
}

class AsyncQueue {
    moodycamel::ConcurrentQueue<std::function<void()>> queue;
    std::vector<std::thread> workers;
    std::atomic<bool> running{true};

public:
    AsyncQueue(size_t threads = 4) {
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                while (running) {
                    std::function<void()> task;
                    if (queue.try_dequeue(task)) task();
                }
            });
        }
    }

    ~AsyncQueue() {
        running = false;
        for (auto& t : workers) t.join();
    }

    template<typename F>
    void enqueue(F&& f) {
        queue.enqueue(std::forward<F>(f));
    }
};

// Использование
async_queue.enqueue([this, pos, data] {
    saveBlockInternal(pos, data);
});

class ConnectionPool {
    std::mutex mutex;
    std::vector<std::unique_ptr<PGconn>> connections;

public:
    PGconn* get() {
        std::lock_guard lock(mutex);
        if (connections.empty()) {
            return PQconnectdb(...);
        }
        auto conn = std::move(connections.back());
        connections.pop_back();
        return conn.release();
    }

    void put(PGconn* conn) {
        std::lock_guard lock(mutex);
        connections.emplace_back(conn);
    }
};

class PGQuery {
    PGconn* m_conn;
public:
    explicit PGQuery(PGconn* conn) : m_conn(conn) {}

    PGResultWrapper exec(const char* query,
                        const std::vector<const char*>& params = {},
                        const std::vector<int>& formats = {})
    {
        PGresult* res = PQexecParams(m_conn, query,
            params.size(), nullptr, params.data(), nullptr,
            formats.empty() ? nullptr : formats.data(), 1);

        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            PQclear(res);
            throw DatabaseException(PQerrorMessage(m_conn));
        }
        return PGResultWrapper(res);
    }
};

// Использование
PGQuery q(m_conn);
auto res = q.exec("INSERT ...", {param1, param2}, {1, 1});

bool CacheManager::get(const std::string & modname,
	const std::string & key,
		std::string & value) {
	std::lock_guard < std::mutex > lock(cache_mutex);
	auto it = cache.find({
		modname,
		key
	});
	if(it != cache.end()) {
		lru_queue.splice(lru_queue.begin(), lru_queue, it->second
		.lru_it);
		// Обновляем LRU
		value = it->second.value;
		return true;
	}
	return false;
}
void CacheManager::put(const std::string& modname,
                      const std::string& key,
                      const std::string& value)
{
    std::lock_guard<std::mutex> lock(cache_mutex);
    KeyType key_pair(modname, key);

    auto it = cache.find(key_pair);
    if (it != cache.end()) {
        // Обновление существующей записи
        lru_queue.erase(it->second.lru_it);
        lru_queue.push_front(key_pair);
        it->second.lru_it = lru_queue.begin();
        it->second.value = value;
        return;
    }

    // Удаление старых записей при превышении лимита
    if (cache.size() >= max_size) {
        auto lru_key = lru_queue.back();
        cache.erase(lru_key);
        lru_queue.pop_back();
    }

    // Добавление новой записи
    lru_queue.push_front(key_pair);
    cache[key_pair] = {value, lru_queue.begin()};
}
void CacheManager::remove(const std::string & modname,
	const std::string & key) {
	std::lock_guard < std::mutex > lock(cache_mutex);
	auto it = cache.find({
		modname,
		key
	});
	if(it != cache.end()) {
		lru_queue.erase(it->second.lru_it);
		cache.erase(it);
	}
}
void CacheManager::purgeMod(const std::string & modname) {
	std::lock_guard < std::mutex > lock(cache_mutex);
	auto it = cache.begin();
	while(it != cache.end()) {
		if(it->first.first == modname) {
			lru_queue.erase(it->second.lru_it);
			it = cache.erase(it);
		}
		else {
			++it;
		}
	}
}
Database_PostgreSQL::Database_PostgreSQL(const std::string & connect_string,
	const char * type): m_connect_string(connect_string) {
	if(m_connect_string.empty()) {
		// Use given type to reference the exact setting in the error message
		std::string s = type;
		std::string msg = "Set pgsql" + s +
			"_connection string in world.mt to "
		"use the postgresql backend\n"
		"Notes:\n"
		"pgsql" + s + "_connection has the following form: \n"
		"\tpgsql" + s + "_connection = host=127.0.0.1 port=5432 "
		"user=mt_user password=mt_password dbname=minetest" + s + "\n"
		"mt_user should have CREATE TABLE, INSERT, SELECT, UPDATE and "
		"DELETE rights on the database. "
		"Don't create mt_user as a SUPERUSER!";
		throw SettingNotFoundException(msg);
	}
}

PGresult * Database_PostgreSQL::execPrepared(const char * stmtName,
	int paramsNumber, const char * const * params, const int * paramsLengths,
	const int * paramsFormats, int resultFormat, bool clear) {
    verifyDatabase();
    PGresult* result = PQexecPrepared(m_conn, stmtName, paramsNumber, params,
        paramsLengths, paramsFormats, resultFormat);
    if (!result) {
        throw DatabaseException("PQexecPrepared failed: " + std::string(PQerrorMessage(m_conn)));
    }
    return checkResults(result, clear);
	}

// Helper function for position serialization
std::string MapDatabasePostgreSQL::posToString(const v3s16 & pos) const {
	return fmt::format("{},{},{}", pos.X, pos.Y, pos.Z);
}
// Реализация prepareStatement
void Database_PostgreSQL::prepareStatement(const char * stmtName,
	const char * query) {
	verifyDatabase();
	PGresult * res = PQprepare(m_conn, stmtName, query, 0, nullptr);
	checkResults(res, true);
}
void Database_PostgreSQL::connectToDatabase() {
	m_conn = PQconnectdb(m_connect_string.c_str());
	if(PQstatus(m_conn) != CONNECTION_OK) {
		std::string err = PQerrorMessage(m_conn);
		PQfinish(m_conn);
		m_conn = nullptr;
		throw DatabaseException("PostgreSQL connection failed: " + err);
	}
	m_pgversion = PQserverVersion(m_conn);
	infostream << "PostgreSQL Database: Version " << m_pgversion <<
		" Connection made." << std::endl;
	createDatabase();
	initStatements();
}
void Database_PostgreSQL::verifyDatabase() {
	if(PQstatus(m_conn) != CONNECTION_OK) {
		throw DatabaseException("Connection lost. Last error: " + std::
			string(PQerrorMessage(m_conn)));
	}
}
void Database_PostgreSQL::ping() {
	if(PQping(m_connect_string.c_str()) != PQPING_OK) {
		throw DatabaseException(std::string("PostgreSQL database error: ") +
			PQerrorMessage(m_conn));
	}
}
void Database_PostgreSQL::reconnectDatabase() {
	if(m_conn) {
		PQfinish(m_conn);
		m_conn =
		nullptr; // Обязательно обнулить перед повторным подключением
	}
	//    PQreset(m_conn);
	//    if (PQstatus(m_conn) != CONNECTION_OK) {
	//        throw DatabaseException("PostgreSQL reconnect failed: " + std::string(PQerrorMessage(m_conn)));
	//    }
	initStatements(); // Переинициализация подготовленных выражений
	connectToDatabase();
}
bool Database_PostgreSQL::initialized() const {
	return m_conn && PQstatus(m_conn) == CONNECTION_OK;
}
PGresult * Database_PostgreSQL::checkResults(PGresult * result, bool clear) {
	ExecStatusType status = PQresultStatus(result);
	if(status == PGRES_FATAL_ERROR || status == PGRES_BAD_RESPONSE) {
		std::string err = PQresultErrorMessage(result);
		PQclear(result);
		throw DatabaseException("Query failed: " + err);
	}
	if(clear) PQclear(result);
	return result;
}
void Database_PostgreSQL::createTableIfNotExists(const std::string & table_name,
	const std::string & definition) {
	const char * params[] = {
		table_name.c_str()
	};
	PGResultWrapper res = execPrepared("table_exists", 1, params);
	if(pg_to_bool(res.get(), 0, 0) == false) {
		prepareStatement("create_table", definition);
		execPrepared("create_table", 0, nullptr, nullptr, nullptr, 0, true);
	}
}
bool Database_PostgreSQL::checkColumnType(
    const std::string& table_name,
    const std::string& column_name,
    const std::string& expected_type
) {
    verifyDatabase();

    try {
        // 1. Подготовка параметров
        const char* params[] = {
            table_name.c_str(),
            column_name.c_str()
        };

        // 2. Выполнение запроса
        PGResultWrapper res = execPrepared(
            "check_column_type",
            2,
            params,
            nullptr,  // lengths
            nullptr,  // formats
            0,        // text result
            false     // don't clear
        );

        // 3. Обработка результатов
        if (PQntuples(res.get()) == 0) {
            return false; // Колонка не существует
        }

        // 4. Сравнение типов
        const std::string actual_type = pg_to_string(res.get(), 0, 0);
        return (actual_type == expected_type);

    } catch (const DatabaseException& e) {
        errorstream << "Column type check failed for "
                   << table_name << "." << column_name << ": "
                   << e.what() << std::endl;
        return false;
    }
}
void Database_PostgreSQL::beginSave() {
	verifyDatabase();
	checkResults(PQexec(m_conn, "BEGIN;"));
}
void Database_PostgreSQL::endSave() {
	checkResults(PQexec(m_conn, "COMMIT;"));
}
void Database_PostgreSQL::rollback() {
	checkResults(PQexec(m_conn, "ROLLBACK;"));
}

void Database_PostgreSQL::initStatements() {
    // ...
    prepareStatement("check_column_type",
        "SELECT udt_name::regtype::text "  // Получаем тип в native form
        "FROM information_schema.columns "
        "WHERE table_name = $1::name "
        "AND column_name = $2::name "
        "LIMIT 1");
    // ...
}

MapDatabasePostgreSQL::MapDatabasePostgreSQL(const std::string& connect_string)
    : Database_PostgreSQL(connect_string, "map"), // Четкое указание типа БД
      MapDatabase(),
      m_cache(validateCacheSize()) // Валидация размера кэша
{
    try {
        // 1. Подключение к базе данных
        connectToDatabase();

        // 2. Проверка активности подключения
        if (!isConnected()) {
            throw DatabaseException("Connection failed for map database");
        }

        // 3. Инициализация кэша данными (опционально)
        initializeCache();

        infostream << "Map database initialized successfully. Cache size: "
                 << m_cache.maxSize() << std::endl;

    } catch (const std::exception& e) {
        errorstream << "Map database initialization error: " << e.what();

        // 4. Гарантированная очистка ресурсов
        shutdownDatabase();
        throw; // Проброс исключения дальше
    }
}

// Валидация размера кэша
size_t MapDatabasePostgreSQL::validateCacheSize() const {
    constexpr size_t DEFAULT_CACHE_SIZE = 100000;
    constexpr size_t MAX_CACHE_SIZE = 100000

void MapDatabasePostgreSQL::initializeCache() {
    PGResultWrapper res = execQuery("SELECT posX, posY, posZ, data FROM blocks");
    const int rows = PQntuples(res.get());

    for (int i = 0; i < rows; ++i) {
        v3s16 pos(
            pg_to_int(res.get(), i, 0),
            pg_to_int(res.get(), i, 1),
            pg_to_int(res.get(), i, 2)
        );
        m_cache.put(posToString(pos), pg_to_string(res.get(), i, 3));
    }
}

bool MapDatabasePostgreSQL::healthCheck() const {
    try {
        PGResultWrapper res = execPrepared("health_check", 0, nullptr);
        return PQresultStatus(res.get()) == PGRES_TUPLES_OK;
    } catch (...) {
        return false;
    }
}

void MapDatabasePostgreSQL::createDatabase() {
    // Создание основных таблиц
    executeSQL(R"(
        CREATE TABLE IF NOT EXISTS blocks (
            posX SMALLINT NOT NULL CHECK (posX BETWEEN -32768 AND 32767),
            posY SMALLINT NOT NULL CHECK (posY BETWEEN -32768 AND 32767),
            posZ SMALLINT NOT NULL CHECK (posZ BETWEEN -32768 AND 32767),
            data BYTEA NOT NULL,
            PRIMARY KEY (posX, posY, posZ)
        )
    )");

    executeSQL(R"(
        CREATE TABLE IF NOT EXISTS schema_info (
            version INTEGER PRIMARY KEY CHECK (version >= 0)
        )
    )");

    // Получение текущей версии схемы
    int current_version = 0;
    {
        PGResultWrapper res = executeQuery(
            "SELECT version FROM schema_info "
            "ORDER BY version DESC LIMIT 1 FOR UPDATE"
        );
        if (PQntuples(res.get()) > 0) {
            current_version = pg_to_int(res.get(), 0, 0);
        }
    }

    // Миграция для версии 1
    if (current_version < 1) {
        PGTransactionGuard transaction(m_conn);
        infostream << "Starting database migration to version 1" << std::endl;

        // Оптимизированная проверка некорректных координат
        executeSQL(R"(
            CREATE INDEX CONCURRENTLY IF NOT EXISTS tmp_coord_check
            ON blocks (posX, posY, posZ)
        )");

        PGResultWrapper res = executeQuery(R"(
            SELECT EXISTS (
                SELECT 1 FROM blocks
                WHERE posX NOT BETWEEN -32768 AND 32767
                   OR posY NOT BETWEEN -32768 AND 32767
                   OR posZ NOT BETWEEN -32768 AND 32767
                LIMIT 1
            )
        )");

        if (pg_to_bool(res.get(), 0, 0)) {
            throw DatabaseException("Invalid coordinate values found");
        }

        // Создание новой таблицы для безопасного изменения типа
        executeSQL(R"(
            CREATE TEMPORARY TABLE blocks_new (LIKE blocks INCLUDING DEFAULTS)
        )");

        // Пакетное копирование данных с использованием курсора
        PGCursor cursor(m_conn, "SELECT posX, posY, posZ, data FROM blocks");
        constexpr int BATCH_SIZE = 1000;
        std::vector<std::string> batch_values;

        while (true) {
            PGResultWrapper chunk = cursor.fetch(BATCH_SIZE);
            if (PQntuples(chunk.get()) == 0) break;

            batch_values.clear();
            for (int i = 0; i < PQntuples(chunk.get()); ++i) {
                batch_values.emplace_back(fmt::format(
                    "({},{},{},'{}')",
                    pg_to_smallint(chunk.get(), i, 0),
                    pg_to_smallint(chunk.get(), i, 1),
                    pg_to_smallint(chunk.get(), i, 2),
                    PQescapeByteaConn(m_conn,
                        PQgetvalue(chunk.get(), i, 3),
                        PQgetlength(chunk.get(), i, 3),
                        nullptr)
                ));
            }

            executeSQL(fmt::format(
                "INSERT INTO blocks_new (posX, posY, posZ, data) VALUES {}",
                fmt::join(batch_values, ",")
            ));
        }

        // Атомарная замена таблиц
        executeSQL(R"(
            DROP TABLE blocks;
            ALTER TABLE blocks_new RENAME TO blocks;
        )");

        // Обновление версии
        executeSQL(R"(
            INSERT INTO schema_info (version)
            VALUES (1)
            ON CONFLICT (version) DO UPDATE SET version = 1
        )");

        transaction.commit();
        infostream << "Migration to version 1 completed" << std::endl;
    }

    // Миграция для версии 2
    if (current_version < 2) {
        PGTransactionGuard transaction(m_conn);
        infostream << "Starting database migration to version 2" << std::endl;

        // Безопасное добавление колонки
        if (!columnExists("blocks", "format_version")) {
            executeSQL(R"(
                ALTER TABLE blocks
                ADD COLUMN format_version SMALLINT NOT NULL DEFAULT 1
            )");
        }

        // Пакетное обновление для снятия DEFAULT
        executeSQL(R"(
            ALTER TABLE blocks
            ALTER COLUMN format_version DROP DEFAULT
        )");

        executeSQL(R"(
            UPDATE schema_info SET version = 2
            WHERE version = 1
        )");

        transaction.commit();
        infostream << "Migration to version 2 completed" << std::endl;
    }
}


// Вспомогательные методы
namespace {
    struct PGTransactionGuard {
        PGconn* conn;
        bool committed = false;

        PGTransactionGuard(PGconn* c) : conn(c) {
            PQexec(conn, "BEGIN");
        }

        ~PGTransactionGuard() {
            if (!committed) {
                PQexec(conn, "ROLLBACK");
            }
        }

        void commit() {
            PQexec(conn, "COMMIT");
            committed = true;
        }
    };

    std::tuple<int16_t, int16_t, int16_t> readCoordinates(PGresult* res, int row) {
        return {
            pg_to_smallint(res, row, 0),
            pg_to_smallint(res, row, 1),
            pg_to_smallint(res, row, 2)
        };
    }

    std::string readBlockData(PGresult* res, int row) {
        const uint8_t* bin_data = reinterpret_cast<const uint8_t*>(PQgetvalue(res, row, 3));
        size_t data_len = PQgetlength(res, row, 3);
        return {bin_data, bin_data + data_len};
    }

    void executeBatchUpdate(
        PGconn* conn,
        const std::vector<std::tuple<int16_t, int16_t, int16_t, std::string, int16_t, int16_t, int16_t>>& batch
    ) {
        std::vector<const char*> params;
        std::vector<int> param_lengths;
        std::vector<int> param_formats;

        for (const auto& [newX, newY, newZ, data, oldX, oldY, oldZ] : batch) {
            params.insert(params.end(), {
                reinterpret_cast<const char*>(&newX),
                reinterpret_cast<const char*>(&newY),
                reinterpret_cast<const char*>(&newZ),
                data.c_str(),
                reinterpret_cast<const char*>(&oldX),
                reinterpret_cast<const char*>(&oldY),
                reinterpret_cast<const char*>(&oldZ)
            });

            param_lengths.insert(param_lengths.end(), {
                sizeof(int16_t),
                sizeof(int16_t),
                sizeof(int16_t),
                static_cast<int>(data.size()),
                sizeof(int16_t),
                sizeof(int16_t),
                sizeof(int16_t)
            });

            param_formats.insert(param_formats.end(), 7, 1);
        }

        PGResultWrapper res(PQexecParams(
            conn,
            "UPDATE blocks SET posX=$1, posY=$2, posZ=$3, data=$4 "
            "WHERE posX=$5 AND posY=$6 AND posZ=$7",
            7 * batch.size(),
            nullptr,
            params.data(),
            param_lengths.data(),
            param_formats.data(),
            1
        ));

        if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
            throw DatabaseException("Batch update failed: " + std::string(PQresultErrorMessage(res.get())));
        }
    }
}

bool MapDatabasePostgreSQL::saveBlock(const v3s16& pos, std::string_view data) {
    // Генерация ключа кеша
    constexpr size_t KEY_BUFFER_SIZE = 64;
    char key_buffer[KEY_BUFFER_SIZE];
    snprintf(key_buffer, sizeof(key_buffer), "%d,%d,%d", pos.X, pos.Y, pos.Z);

    // Обновление кеша
    m_cache.put("map_blocks", key_buffer, std::string(data));

    // Проверка размера данных
    if (data.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        errorstream << "Block data size exceeds limits at " << pos
                  << " (size: " << data.size() << ")" << std::endl;
        return false;
    }

    // Подготовка координат в сетевом порядке байтов
    const int16_t coords[3] = {
        static_cast<int16_t>(pos.X),
        static_cast<int16_t>(pos.Y),
        static_cast<int16_t>(pos.Z)
    };

    #ifdef WORDS_LITTLEENDIAN
    const int16_t network_coords[3] = {
        htons(coords[0]),
        htons(coords[1]),
        htons(coords[2])
    };
    #else
    const int16_t* network_coords = coords;
    #endif

    // Параметры запроса
    const std::array<const char*, 4> params = {
        reinterpret_cast<const char*>(network_coords),
        reinterpret_cast<const char*>(network_coords + 1),
        reinterpret_cast<const char*>(network_coords + 2),
        data.data()
    };

    const std::array<int, 4> lengths = {
        sizeof(int16_t),
        sizeof(int16_t),
        sizeof(int16_t),
        static_cast<int>(data.size())
    };

    constexpr std::array<int, 4> formats = {1, 1, 1, 1}; // Все бинарные

    try {
        PGResultWrapper result = execPrepared(
            "write_block",
            params.size(),
            params.data(),
            lengths.data(),
            formats.data(),
            1,  // Бинарный результат
            true // Автоочистка
        );

        return true;
    } catch (const DatabaseException& e) {
        errorstream << "Failed to save block at " << pos << ": " << e.what()
                  << "\nData size: " << data.size()
                  << "\nParams: " << coords[0] << "," << coords[1] << "," << coords[2];
        return false;
    }
}

class BlockWriteBatch {
    std::vector<std::tuple<v3s16, std::string>> queue;

    void add(const v3s16& pos, std::string_view data) {
        queue.emplace_back(pos, std::string(data));
    }

    void commit() {
        // Пакетная запись в БД
    }
};

void MapDatabasePostgreSQL::loadBlock(const v3s16 & pos, std::string * block) {
	int16_t x = pos.X;
	int16_t y = pos.Y;
	int16_t z = pos.Z;
	#ifdef WORDS_LITTLEENDIAN
	x = htons(x);
	y = htons(y);
	z = htons(z);
	#endif
	const void * args[] = {
		& x,
		& y,
		& z
	};
	const int argLen[] = {
		sizeof(x),
		sizeof(y),
		sizeof(z)
	};
	const int argFmt[] = {
		1,
		1,
		1
	};
	PGResultWrapper results = execPrepared("read_block", std::size(args),
		reinterpret_cast <
		const char *
			const * > (args),
			argLen,
			argFmt,
			1,
			false);
	if(PQntuples(results.get())) * block = std::string(PQgetvalue(results
		.get(), 0, 0), PQgetlength(results.get(), 0, 0)); // Бинарные данные
	else block->clear();
	std::string key = posToString(pos);
	m_cache.put("map_blocks", key, std::string(*block)); // Use 3 arguments
}
bool MapDatabasePostgreSQL::deleteBlock(const v3s16 & pos) {
	std::string key = fmt::format("{},{},{}", pos.X, pos.Y, pos.Z);
	m_cache.remove("map_blocks", key); // Use 2 arguments
	verifyDatabase();
	// Используем int16_t вместо s32
	int16_t x = pos.X;
	int16_t y = pos.Y;
	int16_t z = pos.Z;
	#ifdef WORDS_LITTLEENDIAN
	x = htons(x);
	y = htons(y);
	z = htons(z);
	#endif
	const void * args[] = {
		& x,
		& y,
		& z
	};
	const int argLen[] = {
		sizeof(x),
		sizeof(y),
		sizeof(z)
	};
	const int argFmt[] = {
		1,
		1,
		1
	};
	execPrepared("delete_block", 3, args, argLen, argFmt,
		1, // resultFormat (binary)
		true // clear
	);
	return true;
}
void MapDatabasePostgreSQL::listAllLoadableBlocks(std::vector<v3s16> &dst) {
    verifyDatabase();
    PGResultWrapper results_wrapper = execPrepared("list_all_loadable_blocks", 0,
        nullptr, nullptr, nullptr, 0, false);
    PGresult* results = results_wrapper.get();

    int numrows = PQntuples(results);
    for(int row = 0; row < numrows; ++row)
        dst.push_back(pg_to_v3s16(results, row, 0));

    // Автоматическое освобождение через деструктор PGResultWrapper
}

void MapDatabasePostgreSQL::initStatements() {
	// Prepare SQL statements for map block operations
	prepareStatement("write_block",
		"INSERT INTO blocks (posX, posY, posZ, data) "
		"VALUES ($1::smallint, $2::smallint, $3::smallint, $4::bytea) "
		"ON CONFLICT (posX, posY, posZ) DO UPDATE SET data = EXCLUDED.data"
		);
	prepareStatement("read_block", "SELECT data FROM blocks "
		"WHERE posX = $1::smallint AND posY = $2::smallint AND posZ = $3::smallint"
		);
	prepareStatement("delete_block", "DELETE FROM blocks "
		"WHERE posX = $1::smallint AND posY = $2::smallint AND posZ = $3::smallint"
		);
	prepareStatement("list_all_loadable_blocks",
		"SELECT posX, posY, posZ FROM blocks");
	infostream <<
		"PostgreSQL: Map Database prepared statements initialized." << std::
		endl;
	prepareStatement("upsert_block",
		"INSERT INTO blocks VALUES ($1,$2,$3,$4) "
		"ON CONFLICT DO UPDATE SET data = EXCLUDED.data");
}
/*
 * Player Database
 */
PlayerDatabasePostgreSQL::PlayerDatabasePostgreSQL(const std::string &
		connect_string): Database_PostgreSQL(connect_string, "_player"),
	PlayerDatabase() {
		connectToDatabase();
	}
void PlayerDatabasePostgreSQL::createDatabase() {
	// 1. Основная таблица игроков
	createTableIfNotExists("player", "CREATE TABLE player ("
		"name TEXT PRIMARY KEY, " // Используем TEXT вместо VARCHAR
		"pitch SMALLINT NOT NULL, " // Оптимизация числовых полей
		"yaw SMALLINT NOT NULL, "
		"posX SMALLINT NOT NULL, "
		"posY SMALLINT NOT NULL, "
		"posZ SMALLINT NOT NULL, "
		"hp SMALLINT NOT NULL CHECK (hp BETWEEN 0 AND 32767), "
		"breath SMALLINT NOT NULL CHECK (breath BETWEEN 0 AND 32767), "
		"creation_date TIMESTAMPTZ NOT NULL DEFAULT NOW(), " // Исправление типа времени
		"modification_date TIMESTAMPTZ NOT NULL DEFAULT NOW()"
		");");
	// 2. Инвентари
	createTableIfNotExists("player_inventories",
		"CREATE TABLE player_inventories ("
		"player TEXT REFERENCES player(name) ON DELETE CASCADE, "
		"inv_id SMALLINT NOT NULL, " // Уменьшаем размерность
		"inv_width SMALLINT NOT NULL, "
		"inv_name TEXT NOT NULL DEFAULT '', "
		"inv_size SMALLINT NOT NULL, "
		"PRIMARY KEY(player, inv_id)"
		");");
	// 3. Предметы в инвентаре
	createTableIfNotExists("player_inventory_items",
		"CREATE TABLE player_inventory_items ("
		"player TEXT REFERENCES player(name) ON DELETE CASCADE, "
		"inv_id SMALLINT NOT NULL, "
		"slot_id SMALLINT NOT NULL, " // Оптимизация
		"item TEXT NOT NULL DEFAULT '', "
		"PRIMARY KEY(player, inv_id, slot_id)"
		");");
	// 4. Метаданные
	createTableIfNotExists("player_metadata",
		"CREATE TABLE player_metadata ("
		"player TEXT REFERENCES player(name) ON DELETE CASCADE, "
		"attr TEXT NOT NULL, " // Используем TEXT вместо VARCHAR
		"value TEXT, "
		"PRIMARY KEY(player, attr)"
		");");
	// 5. Дополнительные индексы
	executeSQL(
		"CREATE INDEX IF NOT EXISTS idx_player_modified ON player(modification_date);"
		"CREATE INDEX IF NOT EXISTS idx_metadata_attr ON player_metadata(attr);"
		);
	infostream << "PostgreSQL: Player Database initialized successfully." <<
		std::endl;
}
void PlayerDatabasePostgreSQL::initStatements() {
	// Унифицированный UPSERT-запрос для современных версий PostgreSQL
	prepareStatement("save_player",
		"INSERT INTO player (name, pitch, yaw, posX, posY, posZ, hp, breath, modification_date) "
		"VALUES ($1, $2::smallint, $3::smallint, $4::smallint, $5::smallint, $6::smallint, $7::smallint, $8::smallint, NOW()) "
		"ON CONFLICT (name) DO UPDATE SET "
		"pitch = EXCLUDED.pitch, "
		"yaw = EXCLUDED.yaw, "
		"posX = EXCLUDED.posX, "
		"posY = EXCLUDED.posY, "
		"posZ = EXCLUDED.posZ, "
		"hp = EXCLUDED.hp, "
		"breath = EXCLUDED.breath, "
		"modification_date = NOW()");
	// Базовые операции
	prepareStatement("remove_player", "DELETE FROM player WHERE name = $1");
	prepareStatement("load_player_list", "SELECT name FROM player");
	// Инвентарь
	prepareStatement("add_player_inventory",
		"INSERT INTO player_inventories (player, inv_id, inv_width, inv_name, inv_size) "
		"VALUES ($1, $2::smallint, $3::smallint, $4, $5::smallint) "
		"ON CONFLICT (player, inv_id) DO UPDATE SET "
		"inv_width = EXCLUDED.inv_width, "
		"inv_name = EXCLUDED.inv_name, "
		"inv_size = EXCLUDED.inv_size");
	prepareStatement("add_player_inventory_item",
		"INSERT INTO player_inventory_items (player, inv_id, slot_id, item) "
		"VALUES ($1, $2::smallint, $3::smallint, $4) "
		"ON CONFLICT (player, inv_id, slot_id) DO UPDATE SET "
		"item = EXCLUDED.item");
	// Загрузка данных
	prepareStatement("load_player",
		"SELECT pitch, yaw, posX, posY, posZ, hp, breath, modification_date "
		"FROM player WHERE name = $1");
	// Метаданные
	prepareStatement("save_player_metadata",
		"INSERT INTO player_metadata (player, attr, value) "
		"VALUES ($1, $2, $3) "
		"ON CONFLICT (player, attr) DO UPDATE SET "
		"value = EXCLUDED.value");
	prepareStatement("add_player_inventory_batch",
        "INSERT INTO player_inventory_items "
        "(player, inv_id, slot_id, item) "
        "SELECT * FROM UNNEST($1::TEXT[], $2::SMALLINT[], $3::SMALLINT[], $4::TEXT[])");

    prepareStatement("save_metadata_batch",
        "INSERT INTO player_metadata "
        "(player, attr, value) "
        "SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[], $3::TEXT[]) "
        "ON CONFLICT (player, attr) DO UPDATE SET value = EXCLUDED.value");
}
bool PlayerDatabasePostgreSQL::playerDataExists(const std::string &
playername) {
	const char * values[] = {
		playername.c_str()
	};
	PGResultWrapper results = execPrepared("load_player", 1, values,
		nullptr, nullptr, 0, false);
	bool res = (PQntuples(results.get()) > 0);
	return res;
}
void PlayerDatabasePostgreSQL::savePlayer(RemotePlayer* player) {
    // Проверка корректности объекта SAO
    PlayerSAO* sao = player->getPlayerSAO();
    if (!sao) {
        errorstream << "Attempt to save player with null SAO" << std::endl;
        return;
    }

    try {
        // 1. Подготовка данных
        verifyDatabase();
        const std::string player_name = player->getName(); // Локальная копия для безопасности
        Database_PostgreSQL::beginSave();

        // 2. Сериализация основных данных
        const v3f pos = sao->getBasePosition();

        // 2.1 Проверки диапазонов
        auto safe_cast = [](float value, float scale, const char* field) -> int16_t {
            const float temp = value * scale;
            if (temp < std::numeric_limits<int16_t>::min() ||
                temp > std::numeric_limits<int16_t>::max()) {
                throw std::range_error(std::string("Value overflow for ") + field);
            }
            return static_cast<int16_t>(temp);
        };

        const int16_t pitch = safe_cast(sao->getLookPitch(), 100.0f, "pitch");
        const int16_t yaw = safe_cast(sao->getRotation().Y, 100.0f, "yaw");
        const int16_t pos_x = safe_cast(pos.X, 100.0f, "pos.X");
        const int16_t pos_y = safe_cast(pos.Y, 100.0f, "pos.Y");
        const int16_t pos_z = safe_cast(pos.Z, 100.0f, "pos.Z");
        const int16_t hp = static_cast<int16_t>(sao->getHP());
        const int16_t breath = static_cast<int16_t>(sao->getBreath());

        // 2.2 Подготовка параметров (бинарный формат)
        const std::array<const char*, 8> player_params = {
            player_name.c_str(),
            reinterpret_cast<const char*>(&pitch),
            reinterpret_cast<const char*>(&yaw),
            reinterpret_cast<const char*>(&pos_x),
            reinterpret_cast<const char*>(&pos_y),
            reinterpret_cast<const char*>(&pos_z),
            reinterpret_cast<const char*>(&hp),
            reinterpret_cast<const char*>(&breath)
        };

        // 3. Сохранение основных данных игрока
        execPrepared("save_player",
            player_params.size(),
            player_params.data(),
            nullptr,  // lengths (бинарный формат)
            nullptr,   // formats
            1,        // бинарный результат
            true
        );

        // 4. Удаление старых данных
        const std::array<const char*, 1> del_param = {player_name.c_str()};
        execPrepared("remove_player_inventories", del_param.size(), del_param.data());
        execPrepared("remove_player_inventory_items", del_param.size(), del_param.data());
        execPrepared("remove_player_metadata", del_param.size(), del_param.data());

        // 5. Сохранение инвентаря
        const Inventory* inventory = sao->getInventory();
        std::ostringstream oss;
        oss.reserve(512); // Предварительное выделение памяти

        for (u16 i = 0; i < inventory->getLists().size(); ++i) {
            const InventoryList* list = inventory->getList(i);
            if (!list || list->getSize() == 0) continue;

            // 5.1 Сохранение информации о слоте
            const int16_t inv_id = static_cast<int16_t>(i);
            const int16_t width = static_cast<int16_t>(list->getWidth());
            const int16_t size = static_cast<int16_t>(list->getSize());

            const std::array<const char*, 5> inv_params = {
                player_name.c_str(),
                reinterpret_cast<const char*>(&inv_id),
                reinterpret_cast<const char*>(&width),
                list->getName().c_str(),
                reinterpret_cast<const char*>(&size)
            };

            execPrepared("add_player_inventory",
                inv_params.size(),
                inv_params.data(),
                nullptr,
                nullptr,
                1,
                true
            );

            // 5.2 Пакетное сохранение предметов
            std::vector<const char*> item_params;
            std::vector<std::string> item_data_buffer;

            for (u32 j = 0; j < list->getSize(); ++j) {
                const ItemStack& item = list->getItem(j);
                if (item.empty()) continue;

                // Сериализация предмета
                oss.str("");
                item.serialize(oss);
                item_data_buffer.emplace_back(oss.str());
                const int16_t slot_id = static_cast<int16_t>(j);

                // Подготовка параметров
                item_params.push_back(player_name.c_str());
                item_params.push_back(reinterpret_cast<const char*>(&inv_id));
                item_params.push_back(reinterpret_cast<const char*>(&slot_id));
                item_params.push_back(item_data_buffer.back().c_str());
            }

            // Пакетная вставка через UNNEST
            if (!item_params.empty()) {
                execPreparedBatch("add_player_inventory_batch",
                    item_params.data(),
                    item_params.size() / 4,
                    4
                );
            }
        }

        // 6. Сохранение метаданных
        const StringMap& attrs = sao->getMeta().getStrings();
        std::vector<const char*> meta_params;
        std::vector<std::string> meta_buffer;

        for (const auto& [key, value] : attrs) {
            meta_buffer.push_back(key);
            meta_buffer.push_back(value);

            meta_params.push_back(player_name.c_str());
            meta_params.push_back(meta_buffer[meta_buffer.size()-2].c_str());
            meta_params.push_back(meta_buffer[meta_buffer.size()-1].c_str());
        }

        if (!meta_params.empty()) {
            execPreparedBatch("save_metadata_batch",
                meta_params.data(),
                meta_params.size() / 3,
                3
            );
        }

        // Фиксация транзакции
        Database_PostgreSQL::endSave();
        player->onSuccessfulSave();
    }
    catch (const std::exception& e) {
        Database_PostgreSQL::rollback();
        errorstream << "Player save failed (" << player->getName()
                   << "): " << e.what() << std::endl;
        throw; // Проброс исключения для верхнеуровневой обработки
    }
}

bool PlayerDatabasePostgreSQL::loadPlayer(RemotePlayer* player, PlayerSAO* sao) {
    sanity_check(sao);
    verifyDatabase();

    const std::string& playername = player->getName();
    const char* values[] = { playername.c_str() };

    try {
        // 1. Загрузка основных данных игрока
        PGResultWrapper player_res = execPrepared("load_player", 1, values,
            nullptr, nullptr, 0, false);

        if (PQntuples(player_res.get()) == 0) {
            return false;
        }

        // 2. Парсинг основных данных
        sao->setLookPitch(pg_to_float(player_res.get(), 0, 0));
        sao->setRotation(v3f(0, pg_to_float(player_res.get(), 0, 1), 0));
        sao->setBasePosition(v3f(
            pg_to_float(player_res.get(), 0, 2),
            pg_to_float(player_res.get(), 0, 3),
            pg_to_float(player_res.get(), 0, 4)
        ));
        sao->setHPRaw((u16)pg_to_int(player_res.get(), 0, 5));
        sao->setBreath((u16)pg_to_int(player_res.get(), 0, 6), false);

        // 3. Загрузка инвентарей
        PGResultWrapper inv_res = execPrepared("load_player_inventories", 1, values,
            nullptr, nullptr, 1, false);

        int inv_count = PQntuples(inv_res.get());
        for (int row = 0; row < inv_count; ++row) {
            u16 inv_id = pg_to_uint(inv_res.get(), row, 0);
            u16 width = pg_to_uint(inv_res.get(), row, 1);
            const std::string inv_name = pg_to_string(inv_res.get(), row, 2);
            u16 size = pg_to_uint(inv_res.get(), row, 3);

            InventoryList* invList = player->inventory.addList(inv_name, size);
            invList->setWidth(width);

            // 4. Загрузка предметов инвентаря
            const std::string inv_id_str = std::to_string(inv_id);
            const char* inv_values[] = { playername.c_str(), inv_id_str.c_str() };

            PGResultWrapper item_res = execPrepared("load_player_inventory_items", 2,
                inv_values, nullptr, nullptr, 1, false);

            int item_count = PQntuples(item_res.get());
            for (int item_row = 0; item_row < item_count; item_row++) {
                u16 slot_id = pg_to_uint(item_res.get(), item_row, 0);
                const std::string item_str = pg_to_string(item_res.get(), item_row, 1);

                if (!item_str.empty()) {
                    ItemStack stack;
                    stack.deSerialize(item_str);
                    invList->changeItem(slot_id, stack);
                }
            }
        }

        // 5. Загрузка метаданных
        PGResultWrapper meta_res = execPrepared("load_player_metadata", 1, values,
            nullptr, nullptr, 1, false);

        int meta_count = PQntuples(meta_res.get());
        for (int meta_row = 0; meta_row < meta_count; meta_row++) {
            const std::string key = pg_to_string(meta_res.get(), meta_row, 0);
            const std::string value = pg_to_string(meta_res.get(), meta_row, 1);
            sao->getMeta().setString(key, value);
        }

        sao->getMeta().setModified(false);
        return true;

    } catch (const DatabaseException& e) {
        errorstream << "Player load failed (" << playername
                   << "): " << e.what() << std::endl;
        return false;
    }
}

bool PlayerDatabasePostgreSQL::removePlayer(const std::string & name) {
	if(!playerDataExists(name)) return false;
	verifyDatabase();
	const char * values[] = {
		name.c_str()
	};
	execPrepared("remove_player", 1, values, nullptr, nullptr, 0, true);
	return true;
}
void PlayerDatabasePostgreSQL::listPlayers(std::vector<std::string> &res) {
    verifyDatabase();
    PGResultWrapper res_wrapper = execPrepared("load_player_list", 0,
        nullptr, nullptr, nullptr, 0, false);

    if(PQresultStatus(res_wrapper.get()) != PGRES_TUPLES_OK) {
        throw DatabaseException("Query failed");
    }

    int numrows = PQntuples(res_wrapper.get());
    for(int row = 0; row < numrows; row++)
        res.emplace_back(PQgetvalue(res_wrapper.get(), row, 0));

}
AuthDatabasePostgreSQL::AuthDatabasePostgreSQL(const std::string &
		connect_string): Database_PostgreSQL(connect_string, "_auth"),
	AuthDatabase() {
		connectToDatabase();
	}
void AuthDatabasePostgreSQL::createDatabase() {
	createTableIfNotExists("auth", "CREATE TABLE auth ("
		"id SERIAL,"
		"name TEXT UNIQUE,"
		"password TEXT,"
		"last_login INT NOT NULL DEFAULT 0,"
		"PRIMARY KEY (id)"
		");");
	createTableIfNotExists("user_privileges",
		"CREATE TABLE user_privileges ("
		"id INT,"
		"privilege TEXT,"
		"PRIMARY KEY (id, privilege),"
		"CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES auth (id) ON DELETE CASCADE"
		");");
}
void AuthDatabasePostgreSQL::initStatements() {
	prepareStatement("auth_read",
		"SELECT id, name, password, last_login FROM auth WHERE name = $1"
		);
	prepareStatement("auth_write",
		"UPDATE auth SET name = $1, password = $2, last_login = $3 WHERE id = $4"
		);
	prepareStatement("auth_create",
		"INSERT INTO auth (name, password, last_login) VALUES ($1, $2, $3) RETURNING id"
		);
	prepareStatement("auth_delete", "DELETE FROM auth WHERE name = $1");
	prepareStatement("auth_list_names",
		"SELECT name FROM auth ORDER BY name DESC");
	prepareStatement("auth_read_privs",
		"SELECT privilege FROM user_privileges WHERE id = $1");
	prepareStatement("auth_write_privs",
		"INSERT INTO user_privileges (id, privilege) VALUES ($1, $2)");
	prepareStatement("auth_delete_privs",
		"DELETE FROM user_privileges WHERE id = $1");
}

bool AuthDatabasePostgreSQL::getAuth(const std::string& name, AuthEntry& res) {
    verifyDatabase();

    // 1. Основные данные пользователя
    const char* auth_params[] = { name.c_str() };
    PGResultWrapper auth_res = execPrepared(
        "auth_read",
        1,
        auth_params,
        nullptr,  // lengths (text format)
        nullptr,  // formats (text)
        0,        // result format (text)
        false     // don't clear result
    );

    // Проверка наличия записи
    if (PQntuples(auth_res.get()) == 0) {
        return false;
    }

    // 2. Парсинг основных данных
    parseAuthData(auth_res.get(), res);

    // 3. Получение привилегий через бинарный параметр
    const int32_t user_id = res.id;
    const char* priv_param = reinterpret_cast<const char*>(&user_id);
    const int param_len = sizeof(user_id);

    const char* priv_params[] = { priv_param };
    const int param_lengths[] = { param_len };
    const int param_formats[] = { 1 };  // binary format

    PGResultWrapper privs_res = execPrepared(
        "auth_read_privs",
        1,
        priv_params,
        param_lengths,
        param_formats,
        1,       // binary result
        false
    );

    // Проверка статуса запроса
    if (PQresultStatus(privs_res.get()) != PGRES_TUPLES_OK) {
        throw DatabaseException("Privileges query failed: " +
            std::string(PQresultErrorMessage(privs_res.get())));
    }

    // 4. Парсинг привилегий
    parsePrivileges(privs_res.get(), res.privileges);

    return true;
}

// Вспомогательные методы
namespace {
    void parseAuthData(PGresult* res, AuthEntry& entry) {
        entry.id = pg_to_uint(res, 0, 0);
        entry.name = pg_to_string(res, 0, 1);
        entry.password = pg_to_string(res, 0, 2);
        entry.last_login = pg_to_int(res, 0, 3);
    }

    void parsePrivileges(PGresult* res, std::vector<std::string>& privileges) {
        const int rows = PQntuples(res);
        privileges.reserve(rows);

        for (int i = 0; i < rows; ++i) {
            if (PQgetisnull(res, i, 0)) continue;
            privileges.emplace_back(PQgetvalue(res, i, 0));
        }
    }
}

bool AuthDatabasePostgreSQL::saveAuth(const AuthEntry& authEntry) {
    try {
        verifyDatabase();
        PGTransactionGuard transaction(m_conn); // RAII для транзакции

        // 1. Подготовка параметров в бинарном формате
        const std::array<const char*, 4> params = {
            authEntry.name.c_str(),
            authEntry.password.c_str(),
            reinterpret_cast<const char*>(&authEntry.last_login),
            reinterpret_cast<const char*>(&authEntry.id)
        };

        // 2. Длины параметров (для бинарных данных)
        const std::array<int, 4> lengths = {
            static_cast<int>(authEntry.name.size()),
            static_cast<int>(authEntry.password.size()),
            sizeof(authEntry.last_login),
            sizeof(authEntry.id)
        };

        // 3. Форматы параметров (1 = binary)
        const std::array<int, 4> formats = {0, 0, 1, 1};

        // 4. Выполнение запроса
        execPrepared(
            "auth_update",
            params.size(),
            params.data(),
            lengths.data(),
            formats.data(),
            1,  // binary result format
            true
        );

        // 5. Обновление привилегий
        writePrivileges(authEntry);

        // 6. Фиксация транзакции
        transaction.commit();
        return true;

    } catch (const DatabaseException& e) {
        errorstream << "Failed to save auth entry: " << e.what()
                   << "\nEntry: " << authEntry.name
                   << " (ID: " << authEntry.id << ")";
        return false;
    }
}

struct PGTransactionGuard {
    PGconn* conn;
    bool committed = false;

    PGTransactionGuard(PGconn* c) : conn(c) { PQexec(conn, "BEGIN"); }
    ~PGTransactionGuard() { if (!committed) PQexec(conn, "ROLLBACK"); }
    void commit() { PQexec(conn, "COMMIT"); committed = true; }
};


bool AuthDatabasePostgreSQL::createAuth(AuthEntry& authEntry) {
    try {
        // Валидация входных данных
        if (authEntry.name.empty() || authEntry.name.size() > MAX_NAME_LENGTH) {
            throw DatabaseException("Invalid username length: " + authEntry.name);
        }

        verifyDatabase();
        PGTransactionGuard transaction(m_conn); // RAII для транзакции

        // Подготовка параметров в бинарном формате
        const std::array<const char*, 3> params = {
            authEntry.name.c_str(),
            authEntry.password.c_str(),
            reinterpret_cast<const char*>(&authEntry.last_login)
        };

        const std::array<int, 3> lengths = {
            static_cast<int>(authEntry.name.size()),
            static_cast<int>(authEntry.password.size()),
            sizeof(authEntry.last_login)
        };

        const std::array<int, 3> formats = {0, 0, 1}; // 1 - binary для last_login

        // Выполнение запроса
        PGResultWrapper result = execPrepared(
            "auth_create",
            params.size(),
            params.data(),
            lengths.data(),
            formats.data(),
            1,  // binary result format
            false
        );

        // Проверка результата
        checkQueryResult(result.get(), PGRES_TUPLES_OK, "Auth creation failed");
        checkResultStructure(result.get(), 1, 1);

        // Получение ID
        authEntry.id = extractInsertId(result.get(), 0, 0);

        // Запись привилегий
        writePrivileges(authEntry);

        transaction.commit();
        return true;

    } catch (const DatabaseException& e) {
        errorstream << "Auth creation error [" << authEntry.name
                  << "]: " << e.what()
                  << "\nParams: name=" << authEntry.name
                  << " last_login=" << authEntry.last_login;
        Database_PostgreSQL::rollback();
        return false;
    }
}

// Проверка статуса запроса
void checkQueryResult(PGresult* res, ExecStatusType expected, const std::string& context) {
    const ExecStatusType status = PQresultStatus(res);
    if (status != expected) {
        throw DatabaseException(context + ": " + PQresultErrorMessage(res));
    }
}

// Проверка структуры результата
void checkResultStructure(PGresult* res, int expectedRows, int expectedCols) {
    if (PQntuples(res) != expectedRows || PQnfields(res) < expectedCols) {
        throw DatabaseException("Unexpected result shape. Got: "
            + std::to_string(PQntuples(res)) + " rows, "
            + std::to_string(PQnfields(res)) + " columns");
    }
}

// Извлечение ID из результата
uint32_t extractInsertId(PGresult* res, int row, int col) {
    if (PQgetisnull(res, row, col)) {
        throw DatabaseException("NULL value in ID field");
    }

    const char* idStr = PQgetvalue(res, row, col);
    try {
        return std::stoul(idStr);
    } catch (const std::exception& e) {
        throw DatabaseException("Invalid ID format: " + std::string(idStr));
    }
}

bool AuthDatabasePostgreSQL::deleteAuth(const std::string& name) {
    try {
        // 1. Валидация входных данных
        if (name.empty() || name.length() > MAX_NAME_LENGTH) {
            throw DatabaseException("Invalid username: " + name);
        }

        verifyDatabase();
        PGTransactionGuard transaction(m_conn); // RAII для транзакции

        // 2. Проверка существования записи
        if (!userExists(name)) {
            infostream << "User not found: " << name << std::endl;
            return false;
        }

        // 3. Подготовка параметров в бинарном формате
        const std::array<const char*, 1> params = {name.c_str()};
        const std::array<int, 1> lengths = {static_cast<int>(name.size())};
        const std::array<int, 1> formats = {0}; // text format

        // 4. Выполнение запроса
        PGResultWrapper result = execPrepared(
            "auth_delete",
            params.size(),
            params.data(),
            lengths.data(),
            formats.data(),
            1,  // binary result format
            false
        );

        // 5. Проверка количества удалённых записей
        const char* cmdTuples = PQcmdTuples(result.get());
        const int affectedRows = cmdTuples ? std::atoi(cmdTuples) : 0;

        if (affectedRows == 0) {
            warningstream << "No rows affected when deleting user: " << name << std::endl;
            return false;
        }

        // 6. Фиксация транзакции и логирование
        transaction.commit();
        infostream << "Successfully deleted user: " << name << std::endl;
        return true;

    } catch (const DatabaseException& e) {
        errorstream << "Failed to delete user [" << name << "]: " << e.what() << std::endl;
        Database_PostgreSQL::rollback();
        return false;
    } catch (const std::exception& e) {
        errorstream << "Unexpected error when deleting user [" << name << "]: " << e.what() << std::endl;
        Database_PostgreSQL::rollback();
        return false;
    }
}

// Вспомогательная функция проверки существования пользователя
bool AuthDatabasePostgreSQL::userExists(const std::string& name) const {
    const std::array<const char*, 1> params = {name.c_str()};
    const std::array<int, 1> lengths = {static_cast<int>(name.size())};
    const std::array<int, 1> formats = {0};

    PGResultWrapper result = execPrepared(
        "auth_exists_check",
        1,
        params.data(),
        lengths.data(),
        formats.data(),
        1,
        false
    );

    return PQntuples(result.get()) > 0 && !PQgetisnull(result.get(), 0, 0);
}

struct PGTransactionGuard {
    PGconn* conn;
    bool committed = false;

    explicit PGTransactionGuard(PGconn* c) : conn(c) {
        PQexec(conn, "BEGIN");
    }

    ~PGTransactionGuard() {
        if (!committed) {
            PQexec(conn, "ROLLBACK");
        }
    }

    void commit() {
        PQexec(conn, "COMMIT");
        committed = true;
    }
};

void AuthDatabasePostgreSQL::listNames(std::vector<std::string>& res) {
    try {
        // 1. Проверка соединения с базой
        verifyDatabase();

        // 2. Выполнение подготовленного запроса
        PGResultWrapper results_wrapper = execPrepared(
            "auth_list_names",
            0,              // Количество параметров
            nullptr,        // Параметры
            nullptr,        // Длины параметров
            nullptr,        // Форматы параметров
            0,              // Формат результата (0 - text)
            false           // Не очищать результат автоматически
        );

        // 3. Проверка статуса запроса
        const ExecStatusType status = PQresultStatus(results_wrapper.get());
        if (status != PGRES_TUPLES_OK) {
            throw DatabaseException(
                "Query failed: " + std::string(PQresultErrorMessage(results_wrapper.get()))
            );
        }

        // 4. Оптимизация выделения памяти
        const int numrows = PQntuples(results_wrapper.get());
        res.reserve(res.size() + numrows);

        // 5. Обработка результатов
        for (int row = 0; row < numrows; ++row) {
            // Проверка на NULL в значении
            if (PQgetisnull(results_wrapper.get(), row, 0)) {
                warningstream << "Null username found at row " << row << ", skipping";
                continue;
            }

            // Безопасное извлечение значения
            const char* value = PQgetvalue(results_wrapper.get(), row, 0);
            if (value) {
                res.emplace_back(value);
            } else {
                warningstream << "Invalid data in row " << row << ", skipping";
            }
        }

    } catch (const DatabaseException& e) {
        errorstream << "Failed to list users: " << e.what();
        throw;  // Проброс исключения на уровень выше
    } catch (const std::exception& e) {
        errorstream << "Unexpected error: " << e.what();
        throw DatabaseException("List names operation failed");
    }
}

void AuthDatabasePostgreSQL::reload() {
	// noop for PgSQL
}

void AuthDatabasePostgreSQL::writePrivileges(const AuthEntry& authEntry) {
    try {
        verifyDatabase();
        PGTransactionGuard transaction(m_conn); // RAII-обёртка для транзакции

        // 1. Удаление старых привилегий
        deleteExistingPrivileges(authEntry.id);

        // 2. Пакетная вставка новых привилегий
        if (!authEntry.privileges.empty()) {
            insertPrivilegesBatch(authEntry.id, authEntry.privileges);
        }

        transaction.commit();
    } catch (const DatabaseException& e) {
        errorstream << "Failed to update privileges for user ID "
                  << authEntry.id << ": " << e.what();
        Database_PostgreSQL::rollback();
        throw;
    }
}

// Удаление существующих привилегий
void AuthDatabasePostgreSQL::deleteExistingPrivileges(uint32_t userId) {
    const std::array<const char*, 1> params = {
        reinterpret_cast<const char*>(&userId)
    };

    const std::array<int, 1> lengths = {sizeof(userId)};
    const std::array<int, 1> formats = {1}; // binary format

    execPrepared(
        "auth_delete_privs",
        params.size(),
        params.data(),
        lengths.data(),
        formats.data(),
        1,  // binary result
        true
    );
}

// Пакетная вставка привилегий
void AuthDatabasePostgreSQL::insertPrivilegesBatch(
    uint32_t userId,
    const std::vector<std::string>& privileges
) {
    // Подготовка массивов для UNNEST
    std::vector<uint32_t> userIds(privileges.size(), userId);
    std::vector<const char*> privPointers;
    privPointers.reserve(privileges.size());

    for (const auto& priv : privileges) {
        if (priv.empty() || priv.size() > MAX_PRIV_LENGTH) {
            throw DatabaseException("Invalid privilege: " + priv);
        }
        privPointers.push_back(priv.c_str());
    }

    // Параметры запроса
    const std::array<const char*, 2> params = {
        reinterpret_cast<const char*>(userIds.data()),
        reinterpret_cast<const char*>(privPointers.data())
    };

    const std::array<int, 2> lengths = {
        static_cast<int>(userIds.size() * sizeof(uint32_t)),
        static_cast<int>(privPointers.size() * MAX_PRIV_LENGTH)
    };

    const std::array<int, 2> formats = {1, 0}; // binary для ID, text для привилегий

    execPrepared(
        "auth_insert_privs_batch",
        params.size(),
        params.data(),
        lengths.data(),
        formats.data(),
        1,  // binary result
        true
    );
}

ModStorageDatabasePostgreSQL::ModStorageDatabasePostgreSQL(const std::string& connect_string)
    : Database_PostgreSQL(connect_string, "_mod_storage"),
      ModStorageDatabase(),
      m_cache(validateCacheSize()),
      m_running(false),
      m_write_queue(),
      m_write_mutex(),
      m_write_cv(),
      m_writer_thread()
{
    try {
        // 1. Подключение к базе данных
        connectToDatabase();

        // 2. Проверка валидности подключения
        if (!isConnected()) {
            throw DatabaseException("Failed to establish database connection");
        }

        // 3. Запуск потока для асинхронной записи
        m_running.store(true, std::memory_order_release);
        m_writer_thread = std::thread(&ModStorageDatabasePostgreSQL::writerThread, this);

    } catch (const std::exception& e) {
        // 4. Логирование ошибки и остановка потока
        errorstream << "Initialization failed: " << e.what();
        shutdownThread();
        throw; // Проброс исключения для обработки на верхнем уровне
    }
}

// Деструктор с гарантией остановки потока
ModStorageDatabasePostgreSQL::~ModStorageDatabasePostgreSQL() {
    shutdownThread();
}

// Метод для безопасной остановки потока
void ModStorageDatabasePostgreSQL::shutdownThread() noexcept {
    try {
        m_running.store(false, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(m_write_mutex);
            m_write_cv.notify_all(); // Пробуждаем поток для завершения
        }
        if (m_writer_thread.joinable()) {
            m_writer_thread.join(); // Безопасное присоединение потока
        }
    } catch (const std::exception& e) {
        errorstream << "Thread shutdown error: " << e.what();
    }
}

// Вспомогательная функция для валидации размера кэша
size_t ModStorageDatabasePostgreSQL::validateCacheSize() const {
    constexpr size_t DEFAULT_CACHE_SIZE = 100000;
    constexpr size_t MIN_CACHE_SIZE = 1;

    size_t size = DEFAULT_CACHE_SIZE;

    if (g_settings->exists("mod_storage_cache_size")) {
        int64_t val = g_settings->getS64("mod_storage_cache_size");

        if (val < 0) {
            errorstream << "Invalid cache size: " << val
                      << ". Using default " << DEFAULT_CACHE_SIZE << std::endl;
        } else {
            size = static_cast<size_t>(std::max<int64_t>(MIN_CACHE_SIZE, val));
        }
    }

    return size;
}

void ModStorageDatabasePostgreSQL::shutdownThread() noexcept {
    m_running = false;
    m_write_cv.notify_all();

    if (m_writer_thread.joinable()) {
        try {
            m_writer_thread.join();
        } catch (const std::exception& e) {
            errorstream << "Error joining writer thread: " << e.what() << std::endl;
        }
    }
}

void ModStorageDatabasePostgreSQL::writeBatch(
    const std::vector<std::tuple<std::string, std::string, std::string>>& batch)
{
    std::vector<const char*> modnames, keys, values;
    for (const auto& [m, k, v] : batch) {
        modnames.push_back(m.c_str());
        keys.push_back(k.c_str());
        values.push_back(v.c_str());
    }

    const char* params[] = {
        reinterpret_cast<const char*>(modnames.data()),
        reinterpret_cast<const char*>(keys.data()),
        reinterpret_cast<const char*>(values.data())
    };

    execPrepared("upsert_batch", 3, params,
        {modnames.size(), keys.size(), values.size()},
        {1, 1, 1}, 1, true);
}

ModStorageDatabasePostgreSQL::~ModStorageDatabasePostgreSQL() {
    m_running = false;
    m_write_cv.notify_all();
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        m_write_queue.clear(); // Очистка очереди
    }
    if (m_writer_thread.joinable()) {
        m_writer_thread.join();
    }
    shutdownThread();
}

// Новые методы для асинхронной записи:
void ModStorageDatabasePostgreSQL::writerThread() {
    constexpr auto timeout = std::chrono::milliseconds(100);
    constexpr size_t max_batch_size = 1000;
    std::vector<QueueItem> batch;
    batch.reserve(max_batch_size);

    while (true) {
        // 1. Блокировка с таймаутом для проверки условий
        std::unique_lock<std::mutex> lock(m_write_mutex);
        m_write_cv.wait_for(lock, timeout, [this] {
            return !m_write_queue.empty() || !m_running;
        });

        // 2. Проверка флага завершения
        if (!m_running && m_write_queue.empty()) break;
		if (!m_running.load(std::memory_order_acquire)) break;

        // 3. Подготовка батча для обработки
        if (!m_write_queue.empty()) {
            size_t count = 0;
            while (count < max_batch_size && !m_write_queue.empty()) {
                batch.push_back(std::move(m_write_queue.front()));
                m_write_queue.pop();
                ++count;
            }
        }
        lock.unlock(); // Раннее освобождение мьютекса

        // 4. Обработка батча (без блокировки мьютекса)
        if (!batch.empty()) {
            try {
                // 5. Проверка и восстановление соединения
                if (!Database_PostgreSQL::initialized()) {
                    reconnectDatabase();
                }

                // 6. Выполнение транзакции
                Database_PostgreSQL::beginSave();
                processBatch(batch);
                Database_PostgreSQL::endSave();

                // 7. Обновление кэша для успешных записей
                updateCache(batch);

            } catch (const std::exception& e) {
                errorstream << "Batch processing failed: " << e.what()
                          << "\nRetrying in 1 second...";
                Database_PostgreSQL::rollback();
                requeueBatch(batch); // Возврат элементов в очередь
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            batch.clear();
        }
    }

    // 8. Финализация - гарантированная обработка остатков
    try {
        if (!batch.empty()) {
            processBatch(batch);
        }
    } catch (const std::exception& e) {
        errorstream << "Final flush failed: " << e.what();
    }
}

void ModStorageDatabasePostgreSQL::processBatch(
    const std::vector<QueueItem>& batch
) {
    // Использование расширенного синтаксиса INSERT
    std::string query = "INSERT INTO mod_storage (modname, key, value) VALUES ";
    std::vector<const char*> params;

    for (size_t i = 0; i < batch.size(); ++i) {
        query += fmt::format("(${}, ${}, ${}),", i*3+1, i*3+2, i*3+3);
        params.push_back(batch[i].modname.c_str());
        params.push_back(batch[i].key.c_str());
        params.push_back(batch[i].value.c_str());
    }
    query.resize(query.size()-1); // Удаление последней запятой

    PGResultWrapper res = PQexecParams(m_conn, query.c_str(),
        params.size(), nullptr, params.data(), nullptr, nullptr, 0);

    if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
        throw DatabaseException(PQresultErrorMessage(res.get()));
    }
}

void ModStorageDatabasePostgreSQL::updateCache(
    const std::vector<QueueItem>& batch
) {
    std::lock_guard<std::mutex> lock(m_cache_mutex);
    for (const auto& item : batch) {
        m_cache.put(item.modname, item.key, item.value);
    }
}

void ModStorageDatabasePostgreSQL::requeueBatch(
    std::vector<QueueItem>& batch
) {
    std::lock_guard<std::mutex> lock(m_write_mutex);
    for (auto it = batch.rbegin(); it != batch.rend(); ++it) {
        m_write_queue.push_front(std::move(*it));
    }
}

void ModStorageDatabasePostgreSQL::flushWriteQueue() {
    if (!m_conn || PQstatus(m_conn) != CONNECTION_OK) {
        errorstream << "Connection lost during flush. Reconnecting..." << std::endl;
        reconnectDatabase();
        if (!m_conn) return;
    }

    constexpr size_t MAX_BATCH_SIZE = 1000;
    std::vector<QueueItem> batch;

    // 1. Извлечение элементов из очереди
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        size_t count = 0;
        while (!m_write_queue.empty() && count < MAX_BATCH_SIZE) {
            batch.emplace_back(std::move(m_write_queue.front()));
            m_write_queue.pop();
            ++count;
        }
    }

    if (batch.empty()) {
        return;
    }

    try {
        Database_PostgreSQL::beginSave();

        // 2. Формирование параметров запроса
        std::vector<const char*> params;
        params.reserve(batch.size() * 3);

        for (const auto& item : batch) {
            params.push_back(item.modname.c_str());
            params.push_back(item.key.c_str());
            params.push_back(item.value.c_str());
        }

        // 3. Выполнение пакетной вставки
        PGResultWrapper res = execPreparedBatch(
            "upsert_mod_storage_batch",
            params,
            batch.size(),
            3  // Количество параметров на запись
        );

        // 4. Проверка результата
        const ExecStatusType status = PQresultStatus(res.get());
        if (status != PGRES_COMMAND_OK) {
            throw DatabaseException(
                "Batch insert failed: " + std::string(PQresultErrorMessage(res.get()))
            );
        }

        // 5. Фиксация транзакции
        Database_PostgreSQL::endSave();

        // 6. Обновление кэша
        updateCache(batch);

    } catch (const DatabaseException& e) {
        Database_PostgreSQL::rollback();
        errorstream << "Failed to flush write queue: " << e.what() << std::endl;

        // 7. Возврат элементов в очередь при ошибке
        requeueBatch(batch);
        reconnectDatabase();
    }
}

struct QueueItem {
    std::string modname;
    std::string key;
    std::string value;

    QueueItem(std::string m, std::string k, std::string v)
        : modname(std::move(m)),
          key(std::move(k)),
          value(std::move(v))
    {}
};

PGResultWrapper ModStorageDatabasePostgreSQL::execPreparedBatch(
    const char* stmtName,
    const std::vector<const char*>& params,
    size_t batchSize,
    int paramsPerRow
) {
    std::vector<int> paramFormats(params.size(), 0);
    std::vector<int> paramLengths(params.size(), -1);

    return execPrepared(
        stmtName,
        params.size(),
        params.data(),
        paramLengths.data(),
        paramFormats.data(),
        0,
        false
    );
}

void ModStorageDatabasePostgreSQL::updateCache(const std::vector<QueueItem>& batch) {
    std::lock_guard<std::mutex> lock(m_cache_mutex);
    for (const auto& item : batch) {
        m_cache.put(item.modname, item.key, item.value);
    }
}

void ModStorageDatabasePostgreSQL::requeueBatch(std::vector<QueueItem>& batch) {
    std::lock_guard<std::mutex> lock(m_write_mutex);
    for (auto it = batch.rbegin(); it != batch.rend(); ++it) {
        m_write_queue.push_front(std::move(*it));
    }
}



void ModStorageDatabasePostgreSQL::createDatabase() {
    try {
        // 1. Создание основной таблицы
        constexpr const char* table_definition = R"(
            CREATE TABLE mod_storage (
                modname   TEXT    NOT NULL,
                key       BYTEA   NOT NULL,
                value     BYTEA   NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (modname, key)
            )
        )";

        createTableIfNotExists("mod_storage", table_definition);

        // 2. Создание индексов
        executeSQL(R"(
            CREATE INDEX IF NOT EXISTS idx_mod_storage_modname
            ON mod_storage USING BTREE (modname);

            CREATE INDEX IF NOT EXISTS idx_mod_storage_key
            ON mod_storage USING HASH (key);
        )");

        // 3. Проверка версии схемы
        if (!checkSchemaVersion(1)) {
            runMigrations();
        }

        infostream << "PostgreSQL: Mod Storage Database ('"
                 << getDatabaseName()
                 << "') initialized successfully." << std::endl;

    } catch (const DatabaseException& e) {
        errorstream << "Failed to initialize database: " << e.what() << std::endl;
        throw;
    }
}

bool checkSchemaVersion(int expected_version) {
    PGResultWrapper res = executeQuery(
        "SELECT version FROM schema_version WHERE name = 'mod_storage'");
    return pg_to_int(res.get(), 0, 0) == expected_version;
}

void runMigrations() {
    Database_PostgreSQL::beginSave();
    try {
        // Миграция с версии 0 → 1
        executeSQL(R"(
            ALTER TABLE mod_storage
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
        )");

        executeSQL(R"(
            INSERT INTO schema_version (name, version)
            VALUES ('mod_storage', 1)
            ON CONFLICT (name) DO UPDATE SET version = 1
        )");

        Database_PostgreSQL::endSave();
    } catch (...) {
        Database_PostgreSQL::rollback();
        throw;
    }
}

std::string getDatabaseName() const {
    return PQdb(m_conn);
}

void executeSQL(const std::string& query) {
    PGResultWrapper res = PQexec(m_conn, query.c_str());
    if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
        throw DatabaseException(PQresultErrorMessage(res.get()));
    }
}


void ModStorageDatabasePostgreSQL::initStatements() {
    // Основные операции
    prepareStatement("get_value",
        "SELECT value::bytea "  // Явное приведение типа
        "FROM mod_storage "
        "WHERE modname = $1::text "
        "AND key = $2::bytea");

    prepareStatement("get_all_keys",
        "SELECT key::text "     // Явное приведение типа
        "FROM mod_storage "
        "WHERE modname = $1::text");

    prepareStatement("check_exists",
        "SELECT EXISTS ("
        "    SELECT 1 "
        "    FROM mod_storage "
        "    WHERE modname = $1::text "
        "    AND key = $2::bytea"
        ")");

    // Унифицированный UPSERT
    prepareStatement("upsert_single",
        "INSERT INTO mod_storage (modname, key, value) "
        "VALUES ($1::text, $2::bytea, $3::bytea) "
        "ON CONFLICT (modname, key) DO UPDATE SET "
        "value = EXCLUDED.value");

    // Пакетная вставка через UNNEST
    prepareStatement("upsert_batch",
        "INSERT INTO mod_storage (modname, key, value) "
        "SELECT unnest($1::text[]), unnest($2::bytea[]), unnest($3::bytea[]) "
        "ON CONFLICT (modname, key) DO UPDATE SET "
        "value = EXCLUDED.value");

    // Удаление данных
    prepareStatement("delete_entry",
        "DELETE FROM mod_storage "
        "WHERE modname = $1::text "
        "AND key = $2::bytea");

    prepareStatement("delete_all",
        "DELETE FROM mod_storage "
        "WHERE modname = $1::text");

    prepareStatement("list_mods",
        "SELECT DISTINCT modname::text "
        "FROM mod_storage");
}

void ModStorageDatabasePostgreSQL::getModEntries(
    const std::string& modname,
    StringMap* storage
) {
    try {
        // 1. Подготовка параметров запроса
        const char* params[] = { modname.c_str() };
        const int param_lengths[] = { -1 };  // auto-detect length for text
        const int param_formats[] = { 0 };    // text format

        // 2. Выполнение подготовленного запроса
        PGResultWrapper results = execPrepared(
            "get_all",
            std::size(params),
            params,
            param_lengths,
            param_formats,
            0,      // text result format
            false   // don't auto-clear result
        );

        // 3. Проверка статуса выполнения
        const ExecStatusType status = PQresultStatus(results.get());
        if (status != PGRES_TUPLES_OK) {
            throw DatabaseException(
                "Query failed: " + std::string(PQresultErrorMessage(results.get()))
        }

        // 4. Обработка результатов
        const int num_rows = PQntuples(results.get());
        constexpr int KEY_COLUMN = 0;
        constexpr int VALUE_COLUMN = 1;

        for (int row = 0; row < num_rows; ++row) {
            // Проверка NULL-значений
            if (PQgetisnull(results.get(), row, KEY_COLUMN) ||
                PQgetisnull(results.get(), row, VALUE_COLUMN)) {
                errorstream << "NULL value found in row " << row << " for mod: "
                          << modname << std::endl;
                continue;
            }

            // Безопасное извлечение данных
            std::string key = pg_to_string(results.get(), row, KEY_COLUMN);
            std::string value = pg_to_string(results.get(), row, VALUE_COLUMN);

            storage->emplace(std::move(key), std::move(value));
        }

    } catch (const DatabaseException& e) {
        errorstream << "Failed to get mod entries for '" << modname
                   << "': " << e.what() << std::endl;
        throw;
    }
}


void ModStorageDatabasePostgreSQL::getModKeys(
    const std::string& modname,
    std::vector<std::string>* storage
) {
    try {
        verifyDatabase();

        // 1. Подготовка параметров запроса
        const char* params[] = { modname.c_str() };
        const int param_lengths[] = { -1 }; // auto-detect length for text
        const int param_formats[] = { 0 };  // text format

        // 2. Выполнение подготовленного запроса
        PGResultWrapper results = execPrepared(
            "get_all_keys",
            std::size(params),
            params,
            param_lengths,
            param_formats,
            0,     // text result format
            false  // don't auto-clear result
        );

        // 3. Проверка статуса выполнения
        const ExecStatusType status = PQresultStatus(results.get());
        if (status != PGRES_TUPLES_OK) {
            throw DatabaseException(
                "Query failed: " + std::string(PQresultErrorMessage(results.get()))
            );
        }

        // 4. Обработка результатов
        const int num_rows = PQntuples(results.get());
        storage->reserve(storage->size() + num_rows);

        for (int row = 0; row < num_rows; ++row) {
            // Проверка NULL-значений
            if (PQgetisnull(results.get(), row, 0)) {
                errorstream << "NULL key found for mod: " << modname
                          << " at row: " << row << std::endl;
                continue;
            }

            // Извлечение данных с перемещением
            storage->emplace_back(pg_to_string(results.get(), row, 0));
        }

    } catch (const DatabaseException& e) {
        errorstream << "Failed to get mod keys for '" << modname
                   << "': " << e.what() << std::endl;
        throw;
    }
}

bool ModStorageDatabasePostgreSQL::getModEntry(
    const std::string& modname,
    const std::string& key,
    std::string* value
) {
    // 1. Проверка кэша
    if (m_cache.get(modname, key, *value)) {
        return !value->empty(); // Возвращаем false для пустого негативного кэша
    }

    try {
        verifyDatabase();

        // 2. Подготовка параметров запроса
        const char* params[] = {
            modname.c_str(),
            key.c_str()
        };

        const int param_lengths[] = {
            -1, // auto-detect length for text
            static_cast<int>(std::min(key.size(), static_cast<size_t>(INT_MAX)))
        };

        const int param_formats[] = {
            0, // text format для modname
            1  // binary format для key
        };

        // 3. Выполнение запроса
        PGResultWrapper results = execPrepared(
            "get",
            std::size(params),
            params,
            param_lengths,
            param_formats,
            1,  // binary result format
            false
        );

        // 4. Проверка статуса запроса
        const ExecStatusType status = PQresultStatus(results.get());
        if (status != PGRES_TUPLES_OK) {
            throw DatabaseException(
                "Query failed: " + std::string(PQresultErrorMessage(results.get()))
            );
        }

        // 5. Обработка результатов
        const bool found = PQntuples(results.get()) > 0;
        if (found) {
            // Проверка NULL-значения
            if (PQgetisnull(results.get(), 0, 0)) {
                errorstream << "NULL value found for key: " << key
                          << " in mod: " << modname << std::endl;
                *value = "";
            } else {
                // Бинарные данные
                const char* val = PQgetvalue(results.get(), 0, 0);
                const int len = PQgetlength(results.get(), 0, 0);
                *value = std::string(val, len);
            }
            m_cache.put(modname, key, *value);
        } else {
            m_cache.put(modname, key, ""); // Негативный кэш
        }

        return found;

    } catch (const DatabaseException& e) {
        errorstream << "Failed to get mod entry [" << modname
                   << ":" << key << "]: " << e.what() << std::endl;
        throw;
    }
}


bool ModStorageDatabasePostgreSQL::hasModEntry(
    const std::string& modname,
    const std::string& key
) {
    // 1. Проверка кэша
    std::string cached_value;
    if (m_cache.get(modname, key, cached_value)) {
        return !cached_value.empty();
    }

    try {
        verifyDatabase();

        // 2. Подготовка параметров запроса
        const char* params[] = {
            modname.c_str(),
            key.c_str()
        };

        const int param_lengths[] = {
            -1, // auto-detect length for text
            static_cast<int>(std::min(key.size(), static_cast<size_t>(INT_MAX)))
        };

        const int param_formats[] = {
            0, // text format для modname
            1  // binary format для key
        };

        // 3. Выполнение подготовленного запроса
        PGResultWrapper results = execPrepared(
            "has",
            std::size(params),
            params,
            param_lengths,
            param_formats,
            1,  // binary result format
            false
        );

        // 4. Проверка статуса выполнения
        const ExecStatusType status = PQresultStatus(results.get());
        if (status != PGRES_TUPLES_OK) {
            throw DatabaseException(
                "Query failed: " + std::string(PQresultErrorMessage(results.get()))
            );
        }

        // 5. Обработка результатов
        const bool exists = (PQntuples(results.get()) > 0) &&
                           (PQgetlength(results.get(), 0, 0) > 0);

        // 6. Обновление кэша
        if (exists) {
            m_cache.put(modname, key, "cached_placeholder");
        } else {
            m_cache.put(modname, key, "", NEGATIVE_CACHE_TTL);
        }

        return exists;

    } catch (const DatabaseException& e) {
        errorstream << "Error checking entry [" << modname
                   << ":" << key << "]: " << e.what() << std::endl;
        throw;
    }
}


bool ModStorageDatabasePostgreSQL::setModEntry(
    const std::string& modname,
    const std::string& key,
    std::string_view value
) {
    // Создаем строку один раз для переиспользования
    std::string value_str(value);

    // Обновляем кэш и очередь атомарно под мьютексом
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);

        // 1. Добавление в очередь записи
        m_write_queue.emplace(modname, key, std::move(value_str));

        // 2. Обновление кэша
        m_cache.put(modname, key, m_write_queue.back().third);
    }

    // Уведомляем writer thread
    m_write_cv.notify_one();

    return true;
}

bool ModStorageDatabasePostgreSQL::removeModEntry(
    const std::string& modname,
    const std::string& key
) {
    // 1. Удаление из очереди записей
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        auto it = std::remove_if(
            m_write_queue.begin(),
            m_write_queue.end(),
            [&](const auto& entry) {
                return std::get<0>(entry) == modname &&
                       std::get<1>(entry) == key;
            }
        );
        m_write_queue.erase(it, m_write_queue.end());
    }

    // 2. Удаление из кэша
    m_cache.remove(modname, key);

    // 3. Синхронное удаление из БД
    try {
        verifyDatabase();
        const char* args[] = {modname.c_str(), key.c_str()};
        const int argLen[] = {
            -1,
            static_cast<int>(std::min(key.size(), static_cast<size_t>(INT_MAX))
        };
        const int argFmt[] = {0, 1};

        PGResultWrapper res = execPrepared(
            "remove",
            std::size(args),
            args,
            argLen,
            argFmt,
            1,  // binary result
            false
        );

        // Проверка успешности выполнения
        if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
            errorstream << "Delete failed: " << PQerrorMessage(m_conn);
            return false;
        }

        // Получение количества удаленных записей
        const char* tuples = PQcmdTuples(res.get());
        return tuples != nullptr && std::atoi(tuples) > 0;

    } catch (const DatabaseException& e) {
        errorstream << "Remove mod entry error: " << e.what();
        return false;
    }
}


bool ModStorageDatabasePostgreSQL::removeModEntry(
    const std::string& modname,
    const std::string& key
) {
    // 1. Удаление из очереди записей
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        m_write_queue.erase(
            std::remove_if(
                m_write_queue.begin(),
                m_write_queue.end(),
                [&](const auto& entry) {
                    return std::get<0>(entry) == modname &&
                           std::get<1>(entry) == key;
                }
            ),
            m_write_queue.end()
        );
    }

    // 2. Удаление из кэша
    m_cache.remove(modname, key);

    // 3. Подготовка параметров для запроса
    const char* params[] = {
        modname.c_str(),  // $1 - text
        key.c_str()       // $2 - bytea
    };

    const int param_lengths[] = {
        -1,  // Длина для text определяется автоматически
        static_cast<int>(std::min(key.size(), static_cast<size_t>(INT_MAX))
    };

    const int param_formats[] = {
        0,  // text format для modname
        1   // binary format для key
    };

    try {
        verifyDatabase();

        // 4. Выполнение подготовленного выражения
        PGResultWrapper res = execPrepared(
            "remove",  // Имя подготовленного выражения
            2,         // Количество параметров
            params,
            param_lengths,
            param_formats,
            0,         // Текстовый формат результата
            true       // Автоматическая очистка результата
        );

        // 5. Проверка количества удаленных записей
        const char* cmdTuples = PQcmdTuples(res.get());
        if (!cmdTuples) return false;

        try {
            return std::stoi(cmdTuples) > 0;
        } catch (const std::exception& e) {
            errorstream << "Error parsing affected rows: " << e.what();
            return false;
        }

    } catch (const DatabaseException& e) {
        errorstream << "Remove mod entry error: " << e.what();
        return false;
    }
}


void ModStorageDatabasePostgreSQL::listMods(std::vector<std::string> *res) {
    verifyDatabase();
    PGResultWrapper results_wrapper = execPrepared("list", 0,
        nullptr, nullptr, nullptr, 0, false);

    int numrows = PQntuples(results_wrapper.get());
    for(int row = 0; row < numrows; ++row)
        res->push_back(pg_to_string(results_wrapper.get(), row, 0));
}

Database_PostgreSQL::~Database_PostgreSQL() {
	if(m_conn != nullptr) {
		infostream << "Closing PostgreSQL connection: " << m_conn << std::
			endl;
		PQfinish(m_conn);
		m_conn = nullptr;
	}
}
#endif // USE_POSTGRESQL
