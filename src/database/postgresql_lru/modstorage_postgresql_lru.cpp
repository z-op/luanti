#include "mod_storage_database_postgresql.h"
#include "util/string.h"
#include "exceptions.h"
#include <sstream>
#include <vector>

using namespace std::chrono_literals;

ModStorageDatabasePostgreSQL::ModStorageDatabasePostgreSQL(
    const std::string& connect_string,
    size_t cache_size)
    : BaseDatabasePostgreSQL(connect_string, "mod_storage", cache_size)
{
    initStatements();
}

void ModStorageDatabasePostgreSQL::createDatabase()
{
    executeQuery(R"(
        CREATE TABLE IF NOT EXISTS mod_storage (
            modname VARCHAR(64) NOT NULL,
            key VARCHAR(256) NOT NULL,
            value BYTEA NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            modified_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP,
            PRIMARY KEY (modname, key)
        );

        CREATE INDEX IF NOT EXISTS idx_mod_storage_expires
            ON mod_storage USING BRIN (expires_at);
        CREATE INDEX IF NOT EXISTS idx_mod_storage_modname
            ON mod_storage (modname);
    )");
}

void ModStorageDatabasePostgreSQL::initStatements()
{
    prepareStatement(STMT_SET_ENTRY,
        "INSERT INTO mod_storage (modname, key, value, modified_at, expires_at)"
        "VALUES ($1, $2, $3, NOW(), $4)"
        "ON CONFLICT (modname, key) DO UPDATE SET "
        "value = EXCLUDED.value, "
        "modified_at = EXCLUDED.modified_at, "
        "expires_at = EXCLUDED.expires_at");

    prepareStatement(STMT_GET_ENTRY,
        "SELECT value, expires_at FROM mod_storage "
        "WHERE modname = $1 AND key = $2");

    prepareStatement(STMT_REMOVE_ENTRY,
        "DELETE FROM mod_storage WHERE modname = $1 AND key = $2");

    prepareStatement(STMT_REMOVE_ALL,
        "DELETE FROM mod_storage WHERE modname = $1");

    prepareStatement(STMT_LIST_KEYS,
        "SELECT key FROM mod_storage WHERE modname = $1");
}

void ModStorageDatabasePostgreSQL::checkSchemaVersion()
{
    if (!columnExists("mod_storage", "expires_at")) {
        executeQuery("ALTER TABLE mod_storage ADD COLUMN expires_at TIMESTAMP");
    }
}

// Основные операции с данными

void ModStorageDatabasePostgreSQL::setModEntry(
    const std::string& modname,
    const std::string& key,
    const std::string& value,
    std::chrono::seconds ttl)
{
    validateKey(key);
    const std::string cache_key = makeCacheKey(modname, key);

    // Асинхронное обновление кэша
    enqueueAsync([=] {
        putToCache("mod_data", cache_key, value, ttl.count());
    });

    try {
        const time_t expires = ttl.count() > 0 ?
            std::chrono::system_clock::to_time_t(
                std::chrono::system_clock::now() + ttl) : 0;

        executePrepared(STMT_SET_ENTRY, {
            modname.c_str(),
            key.c_str(),
            value.c_str(),
            reinterpret_cast<const char*>(&expires)
        }, {
            0,  // modname (text)
            0,  // key (text)
            0,  // value (binary)
            1   // expires_at (binary timestamp)
        }, {
            0,
            0,
            1,
            1
        });
    } catch (const DatabaseException& e) {
        errorstream << "Failed to set mod entry [" << modname
                  << ":" << key << "]: " << e.what();
        throw;
    }
}

bool ModStorageDatabasePostgreSQL::getModEntry(
    const std::string& modname,
    const std::string& key,
    std::string& value)
{
    validateKey(key);
    const std::string cache_key = makeCacheKey(modname, key);

    // Попытка получить из кэша
    if (getFromCache("mod_data", cache_key, value)) {
        return true;
    }

    try {
        PGresult* res = executePrepared(STMT_GET_ENTRY, {
            modname.c_str(),
            key.c_str()
        }, {}, {0, 0});

        if (PQntuples(res) > 0) {
            // Проверка срока действия
            const time_t expires = *reinterpret_cast<const time_t*>(
                PQgetvalue(res, 0, 1));

            if (expires > 0 && expires < std::time(nullptr)) {
                removeModEntry(modname, key);
                return false;
            }

            // Получение значения
            const char* val = PQgetvalue(res, 0, 0);
            const int len = PQgetlength(res, 0, 0);
            value.assign(val, len);

            // Обновление кэша
            const auto ttl = expires > 0 ?
                std::chrono::seconds(expires - std::time(nullptr)) : 0s;
            putToCache("mod_data", cache_key, value, ttl.count());

            return true;
        }
        return false;
    } catch (const DatabaseException& e) {
        errorstream << "Failed to get mod entry [" << modname
                  << ":" << key << "]: " << e.what();
        throw;
    }
}

void ModStorageDatabasePostgreSQL::removeModEntry(
    const std::string& modname,
    const std::string& key)
{
    validateKey(key);
    const std::string cache_key = makeCacheKey(modname, key);
    removeFromCache("mod_data", cache_key);

    try {
        executePrepared(STMT_REMOVE_ENTRY, {
            modname.c_str(),
            key.c_str()
        });
    } catch (const DatabaseException& e) {
        errorstream << "Failed to remove mod entry [" << modname
                  << ":" << key << "]: " << e.what();
        throw;
    }
}

// Массовые операции

void ModStorageDatabasePostgreSQL::removeAllEntries(const std::string& modname)
{
    purgeCacheCategory("mod_data:" + modname);

    try {
        executePrepared(STMT_REMOVE_ALL, {modname.c_str()});
    } catch (const DatabaseException& e) {
        errorstream << "Failed to remove all entries for mod ["
                  << modname << "]: " << e.what();
        throw;
    }
}

std::vector<std::string> ModStorageDatabasePostgreSQL::listKeys(
    const std::string& modname)
{
    std::vector<std::string> keys;

    try {
        PGresult* res = executePrepared(STMT_LIST_KEYS, {modname.c_str()});
        const int rows = PQntuples(res);
        keys.reserve(rows);

        for (int i = 0; i < rows; ++i) {
            keys.emplace_back(PQgetvalue(res, i, 0));
        }

        return keys;
    } catch (const DatabaseException& e) {
        errorstream << "Failed to list keys for mod ["
                  << modname << "]: " << e.what();
        throw;
    }
}

// Вспомогательные методы

std::string ModStorageDatabasePostgreSQL::makeCacheKey(
    const std::string& modname,
    const std::string& key) const
{
    return modname + ":" + key;
}

void ModStorageDatabasePostgreSQL::validateKey(const std::string& key) const
{
    if (key.empty() || key.size() > 256) {
        throw std::invalid_argument("Invalid mod storage key length");
    }

    if (key.find_first_of("\\/") != std::string::npos) {
        throw std::invalid_argument("Invalid characters in key");
    }
}

void ModStorageDatabasePostgreSQL::cleanupExpired()
{
    try {
        executeQuery("DELETE FROM mod_storage WHERE expires_at < NOW()");
        infostream << "Cleaned up expired mod storage entries";
    } catch (const DatabaseException& e) {
        errorstream << "Mod storage cleanup failed: " << e.what();
    }
}
