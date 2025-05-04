/**
 * @class MapDatabasePostgreSQL
 * @brief Реализация хранения карт в PostgreSQL
 *
 * Поддерживает:
 * - Асинхронное кэширование блоков
 * - Пакетную вставку
 * - Автоматическое восстановление соединений
 *
 * Пример использования:
 * @code
 * MapDatabasePostgreSQL db("host=localhost");
 * db.saveBlock({0,0,0}, "block_data");
 * @endcode
 */

#include "map_database_postgresql.h"
#include "util/string.h"
#include "exceptions.h"
#include <sstream>

using namespace std::chrono_literals;

// Инициализация структуры базы данных
void MapDatabasePostgreSQL::createDatabase() {
    executeQuery(R"(
        CREATE TABLE IF NOT EXISTS map_blocks (
            pos_x SMALLINT NOT NULL CHECK(pos_x BETWEEN -32768 AND 32767),
            pos_y SMALLINT NOT NULL CHECK(pos_y BETWEEN -32768 AND 32767),
            pos_z SMALLINT NOT NULL CHECK(pos_z BETWEEN -32768 AND 32767),
            data BYTEA NOT NULL,
            mtime TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (pos_x, pos_y, pos_z)
        );

        CREATE INDEX IF NOT EXISTS idx_map_blocks_mtime
        ON map_blocks USING BRIN (mtime);
    )");
}

// Подготовка SQL-запросов
void MapDatabasePostgreSQL::initStatements() {
    prepareStatement(STMT_SAVE_BLOCK,
        "INSERT INTO map_blocks (pos_x, pos_y, pos_z, data) "
        "VALUES ($1, $2, $3, $4) "
        "ON CONFLICT (pos_x, pos_y, pos_z) DO UPDATE SET "
        "data = EXCLUDED.data, mtime = NOW()");

    prepareStatement(STMT_LOAD_BLOCK,
        "SELECT data FROM map_blocks "
        "WHERE pos_x = $1 AND pos_y = $2 AND pos_z = $3");

    prepareStatement(STMT_DELETE_BLOCK,
        "DELETE FROM map_blocks "
        "WHERE pos_x = $1 AND pos_y = $2 AND pos_z = $3");

    prepareStatement(STMT_BLOCK_EXISTS,
        "SELECT EXISTS(SELECT 1 FROM map_blocks "
        "WHERE pos_x = $1 AND pos_y = $2 AND pos_z = $3)");

    prepareStatement(STMT_LIST_BLOCKS,
        "SELECT pos_x, pos_y, pos_z FROM map_blocks");
}

// Проверка и миграция схемы
void MapDatabasePostgreSQL::checkSchemaVersion() {
    if (!columnExists("map_blocks", "mtime")) {
        beginTransaction();
        try {
            executeQuery("ALTER TABLE map_blocks ADD COLUMN mtime TIMESTAMP");
            executeQuery("UPDATE map_blocks SET mtime = NOW()");
            commitTransaction();
        } catch (...) {
            rollbackTransaction();
            throw;
        }
    }
}

// Основные методы работы с блоками
bool MapDatabasePostgreSQL::saveBlock(const v3s16& pos, const std::string& data) {
    const std::string cache_key = serializePosition(pos);

    // Асинхронное обновление кэша
    enqueueAsync([this, cache_key, data] {
        putToCache("blocks", cache_key, data);
    });

    try {
        const int16_t coords[3] = {pos.X, pos.Y, pos.Z};
        const std::vector<const char*> params = {
            reinterpret_cast<const char*>(coords),
            data.c_str()
        };
        const std::vector<int> lengths = {
            sizeof(coords),
            static_cast<int>(data.size())
        };

        executePrepared(STMT_SAVE_BLOCK, params, lengths, {1, 0});
        return true;
    } catch (const DatabaseException& e) {
        errorstream << "Block save error at " << pos << ": " << e.what();
        return false;
    }
}

bool MapDatabasePostgreSQL::loadBlock(const v3s16& pos, std::string& data) {
    const std::string cache_key = serializePosition(pos);

    // Попытка получить из кэша
    if (getFromCache("blocks", cache_key, data)) {
        return !data.empty();
    }

    try {
        const int16_t coords[3] = {pos.X, pos.Y, pos.Z};
        PGresult* res = executePrepared(STMT_LOAD_BLOCK,
            {reinterpret_cast<const char*>(coords)},
            {sizeof(coords)},
            {1}
        );

        if (PQntuples(res) > 0) {
            const char* val = PQgetvalue(res, 0, 0);
            const int len = PQgetlength(res, 0, 0);
            data.assign(val, len);
            putToCache("blocks", cache_key, data);
            return true;
        }
        return false;
    } catch (const DatabaseException& e) {
        errorstream << "Block load error at " << pos << ": " << e.what();
        return false;
    }
}

bool MapDatabasePostgreSQL::deleteBlock(const v3s16& pos) {
    const std::string cache_key = serializePosition(pos);
    removeFromCache("blocks", cache_key);

    try {
        const int16_t coords[3] = {pos.X, pos.Y, pos.Z};
        executePrepared(STMT_DELETE_BLOCK,
            {reinterpret_cast<const char*>(coords)},
            {sizeof(coords)},
            {1}
        );
        return true;
    } catch (const DatabaseException& e) {
        errorstream << "Block delete error at " << pos << ": " << e.what();
        return false;
    }
}

// Массовые операции
void MapDatabasePostgreSQL::saveBlocksBatch(
    const std::unordered_map<v3s16, std::string>& blocks)
{
    beginTransaction();
    try {
        for (const auto& [pos, data] : blocks) {
            saveBlock(pos, data);
        }
        commitTransaction();
    } catch (...) {
        rollbackTransaction();
        throw;
    }
}

// Оптимизация таблиц
void MapDatabasePostgreSQL::optimize() {
    executeQuery("VACUUM ANALYZE map_blocks");
    executeQuery("REINDEX TABLE map_blocks");
}
