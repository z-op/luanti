#include "player_database_postgresql.h"
#include "util/serialize.h"
#include "exceptions.h"
#include "irr_v3d.h"
#include <sstream>
#include <vector>

using namespace std::chrono_literals;

PlayerDatabasePostgreSQL::PlayerDatabasePostgreSQL(const std::string& connect_string)
    : BaseDatabasePostgreSQL(connect_string, "player", 100000)
{
    initStatements();
}

void PlayerDatabasePostgreSQL::createDatabase()
{
    executeQuery(R"(
        CREATE TABLE IF NOT EXISTS players (
            name VARCHAR(32) PRIMARY KEY,
            pos_x DOUBLE PRECISION NOT NULL,
            pos_y DOUBLE PRECISION NOT NULL,
            pos_z DOUBLE PRECISION NOT NULL,
            pitch FLOAT NOT NULL,
            yaw FLOAT NOT NULL,
            hp SMALLINT NOT NULL CHECK(hp BETWEEN 0 AND 1000),
            breath SMALLINT NOT NULL CHECK(breath BETWEEN 0 AND 1000),
            inventory BYTEA NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            modified_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS player_metadata (
            player VARCHAR(32) REFERENCES players(name) ON DELETE CASCADE,
            key VARCHAR(64) NOT NULL,
            value TEXT,
            PRIMARY KEY (player, key)
        );

        CREATE INDEX IF NOT EXISTS idx_players_modified ON players(modified_at);
    )");
}

void PlayerDatabasePostgreSQL::initStatements()
{
    // Основные операции с игроками
    prepareStatement(STMT_SAVE_PLAYER,
        "INSERT INTO players (name, pos_x, pos_y, pos_z, pitch, yaw, hp, breath, inventory) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
        "ON CONFLICT (name) DO UPDATE SET "
        "pos_x = EXCLUDED.pos_x, "
        "pos_y = EXCLUDED.pos_y, "
        "pos_z = EXCLUDED.pos_z, "
        "pitch = EXCLUDED.pitch, "
        "yaw = EXCLUDED.yaw, "
        "hp = EXCLUDED.hp, "
        "breath = EXCLUDED.breath, "
        "inventory = EXCLUDED.inventory, "
        "modified_at = NOW()");

    prepareStatement(STMT_LOAD_PLAYER,
        "SELECT pos_x, pos_y, pos_z, pitch, yaw, hp, breath, inventory "
        "FROM players WHERE name = $1");

    prepareStatement(STMT_SAVE_METADATA,
        "INSERT INTO player_metadata (player, key, value) "
        "VALUES ($1, $2, $3) "
        "ON CONFLICT (player, key) DO UPDATE SET "
        "value = EXCLUDED.value");

    prepareStatement(STMT_LOAD_METADATA,
        "SELECT key, value FROM player_metadata WHERE player = $1");

    prepareStatement(STMT_REMOVE_PLAYER,
        "DELETE FROM players WHERE name = $1");
}

void PlayerDatabasePostgreSQL::checkSchemaVersion()
{
    if (!columnExists("players", "modified_at")) {
        executeQuery("ALTER TABLE players ADD COLUMN modified_at TIMESTAMP");
        executeQuery("UPDATE players SET modified_at = NOW()");
    }
}

// Основные операции с игроками

bool PlayerDatabasePostgreSQL::savePlayer(const std::string& name, const PlayerData& data)
{
    const std::string cache_key = "player:" + name;

    try {
        beginTransaction();

        // Сериализация позиции и инвентаря
        std::ostringstream oss;
        data.inventory.serialize(oss);
        const std::string inv_data = oss.str();

        // Основные данные игрока
        executePrepared(STMT_SAVE_PLAYER, {
            name.c_str(),
            std::to_string(data.pos.X).c_str(),
            std::to_string(data.pos.Y).c_str(),
            std::to_string(data.pos.Z).c_str(),
            std::to_string(data.pitch).c_str(),
            std::to_string(data.yaw).c_str(),
            std::to_string(data.hp).c_str(),
            std::to_string(data.breath).c_str(),
            inv_data.c_str()
        }, {}, {0,0,0,0,0,0,0,0,1});

        // Метаданные
        saveMetadataBatch(name, data.metadata);

        commitTransaction();

        // Обновление кэша
        enqueueAsync([=] {
            putToCache("players", cache_key, data);
        });

        return true;
    } catch (const DatabaseException& e) {
        rollbackTransaction();
        errorstream << "Failed to save player " << name << ": " << e.what();
        return false;
    }
}

bool PlayerDatabasePostgreSQL::loadPlayer(const std::string& name, PlayerData& data)
{
    const std::string cache_key = "player:" + name;

    // Попытка получить из кэша
    if (getFromCache("players", cache_key, data)) {
        return true;
    }

    try {
        beginTransaction();

        // Основные данные
        PGresult* res = executePrepared(STMT_LOAD_PLAYER, {name.c_str()});

        if (PQntuples(res) == 0) {
            rollbackTransaction();
            return false;
        }

        data.pos.X = std::stof(PQgetvalue(res, 0, 0));
        data.pos.Y = std::stof(PQgetvalue(res, 0, 1));
        data.pos.Z = std::stof(PQgetvalue(res, 0, 2));
        data.pitch = std::stof(PQgetvalue(res, 0, 3));
        data.yaw = std::stof(PQgetvalue(res, 0, 4));
        data.hp = std::stoi(PQgetvalue(res, 0, 5));
        data.breath = std::stoi(PQgetvalue(res, 0, 6));

        // Десериализация инвентаря
        const std::string inv_data(PQgetvalue(res, 0, 7), PQgetlength(res, 0, 7));
        std::istringstream iss(inv_data);
        data.inventory.deSerialize(iss);

        // Метаданные
        loadMetadata(name, data.metadata);

        commitTransaction();

        // Обновление кэша
        putToCache("players", cache_key, data);

        return true;
    } catch (const DatabaseException& e) {
        rollbackTransaction();
        errorstream << "Failed to load player " << name << ": " << e.what();
        return false;
    }
}

bool PlayerDatabasePostgreSQL::removePlayer(const std::string& name)
{
    const std::string cache_key = "player:" + name;
    removeFromCache("players", cache_key);

    try {
        executePrepared(STMT_REMOVE_PLAYER, {name.c_str()});
        return true;
    } catch (const DatabaseException& e) {
        errorstream << "Failed to remove player " << name << ": " << e.what();
        return false;
    }
}

// Работа с метаданными

void PlayerDatabasePostgreSQL::saveMetadataBatch(
    const std::string& name,
    const std::unordered_map<std::string, std::string>& metadata)
{
    std::vector<const char*> names, keys, values;
    for (const auto& [key, value] : metadata) {
        names.push_back(name.c_str());
        keys.push_back(key.c_str());
        values.push_back(value.c_str());
    }

    if (!names.empty()) {
        executePreparedBatch(STMT_SAVE_METADATA, {
            names.data(),
            keys.data(),
            values.data()
        }, names.size());
    }
}

void PlayerDatabasePostgreSQL::loadMetadata(
    const std::string& name,
    std::unordered_map<std::string, std::string>& metadata)
{
    PGresult* res = executePrepared(STMT_LOAD_METADATA, {name.c_str()});
    const int rows = PQntuples(res);

    for (int i = 0; i < rows; ++i) {
        metadata[PQgetvalue(res, i, 0)] = PQgetvalue(res, i, 1);
    }
}

// Вспомогательные методы

void PlayerDatabasePostgreSQL::validatePlayerName(const std::string& name) const
{
    if (name.empty() || name.size() > 32) {
        throw std::invalid_argument("Invalid player name length");
    }

    if (name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
        != std::string::npos) {
        throw std::invalid_argument("Invalid characters in player name");
    }
}

void PlayerDatabasePostgreSQL::cleanupOldPlayers(std::chrono::years max_age)
{
    try {
        executeQuery(fmt::format(
            "DELETE FROM players WHERE modified_at < NOW() - INTERVAL '{} years'",
            max_age.count()
        ));
    } catch (const DatabaseException& e) {
        errorstream << "Player cleanup failed: " << e.what();
    }
}
