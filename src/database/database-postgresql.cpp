// Luanti
// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (C) 2016 Loic Blot <loic.blot@unix-experience.fr>

#include "config.h"

#if USE_POSTGRESQL

#include "database-postgresql.h"

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
namespace irr {
namespace core {
std::ostream& operator<<(std::ostream& os, const vector3d<short>& v) {
    return os << "(" << v.X << ", " << v.Y << ", " << v.Z << ")";
}
} // namespace core
} // namespace irr


CacheManager::CacheManager(size_t size) : max_size(size) {}

bool CacheManager::get(
    const std::string &modname,
    const std::string &key,
    std::string &value
) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache.find({modname, key});
    if (it != cache.end()) {
		lru_queue.splice(lru_queue.begin(), lru_queue, it->second.lru_it);
        // Обновляем LRU
        value = it->second.value;
        return true;
    }
    return false;
}

void CacheManager::put(
    const std::string &modname,
    const std::string &key,
    const std::string &value
) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    KeyType key_pair(modname, key);

    auto it = cache.find(key_pair);
    if (it != cache.end()) {
        lru_queue.erase(it->second.lru_it);
        lru_queue.push_front(key_pair);
        it->second.lru_it = lru_queue.begin();
        it->second.value = value;
        return;
    }

    if (cache.size() >= max_size) {
        auto lru_key = lru_queue.back();
        cache.erase(lru_key);
        lru_queue.pop_back();
    }

    lru_queue.push_front(key_pair);
    cache[key_pair] = {value, lru_queue.begin()};
}

void CacheManager::remove(const std::string &modname, const std::string &key) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache.find({modname, key});
    if (it != cache.end()) {
        lru_queue.erase(it->second.lru_it);
        cache.erase(it);
    }
}

void CacheManager::purgeMod(const std::string &modname) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache.begin();
    while (it != cache.end()) {
        if (it->first.first == modname) {
            lru_queue.erase(it->second.lru_it);
            it = cache.erase(it);
        } else {
            ++it;
        }
    }
}


Database_PostgreSQL::Database_PostgreSQL(const std::string &connect_string,
	const char *type) :
	m_connect_string(connect_string)
{
	if (m_connect_string.empty()) {
		// Use given type to reference the exact setting in the error message
		std::string s = type;
		std::string msg =
			"Set pgsql" + s + "_connection string in world.mt to "
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

PGresult* Database_PostgreSQL::execPrepared(
    const char* stmtName,
    int paramsNumber,
    const char* const* params,
    const int* paramsLengths,
    const int* paramsFormats,
    int resultFormat,
    bool clear = true
) {
    verifyDatabase();
    PGresult* result = PQexecPrepared(
        m_conn,
        stmtName,
        paramsNumber,
        params,
        paramsLengths,
        paramsFormats,
        resultFormat
    );
    return checkResults(result, clear);
}

// Helper function for position serialization
std::string MapDatabasePostgreSQL::posToString(const v3s16 &pos) const
{
    return fmt::format("{},{},{}", pos.X, pos.Y, pos.Z);
}

// Реализация prepareStatement
void Database_PostgreSQL::prepareStatement(
    const char* stmtName,
    const char* query
) {
    verifyDatabase();
    PGresult* res = PQprepare(m_conn, stmtName, query, 0, nullptr);
    checkResults(res, true);
}

void Database_PostgreSQL::connectToDatabase()
{
	m_conn = PQconnectdb(m_connect_string.c_str());

	if (PQstatus(m_conn) != CONNECTION_OK) {
		throw DatabaseException(std::string(
			"PostgreSQL database error: ") +
			PQerrorMessage(m_conn));
	}

	m_pgversion = PQserverVersion(m_conn);

	infostream << "PostgreSQL Database: Version " << m_pgversion
			<< " Connection made." << std::endl;

	createDatabase();
	initStatements();
}

void Database_PostgreSQL::verifyDatabase()
{
	if (PQstatus(m_conn) == CONNECTION_OK)
		return;

	PQreset(m_conn);
	ping();
}

void Database_PostgreSQL::ping()
{
	if (PQping(m_connect_string.c_str()) != PQPING_OK) {
		throw DatabaseException(std::string(
			"PostgreSQL database error: ") +
			PQerrorMessage(m_conn));
	}
}

void Database_PostgreSQL::reconnectDatabase() {
	    if (m_conn) {
        PQfinish(m_conn);
        m_conn = nullptr; // Обязательно обнулить перед повторным подключением
    }
    PQreset(m_conn);
    if (PQstatus(m_conn) != CONNECTION_OK) {
        throw DatabaseException("PostgreSQL reconnect failed: " + std::string(PQerrorMessage(m_conn)));
    }
    initStatements(); // Переинициализация подготовленных выражений
	connectToDatabase();
}

bool Database_PostgreSQL::initialized() const
{
	return m_conn && PQstatus(m_conn) == CONNECTION_OK;
}

PGresult *Database_PostgreSQL::checkResults(PGresult *result, bool clear)
{
	ExecStatusType statusType = PQresultStatus(result);

	switch (statusType) {
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK:
		break;
	case PGRES_FATAL_ERROR:
	default:
		throw DatabaseException(
			std::string("PostgreSQL database error: ") +
			PQresultErrorMessage(result));
	}

	if (clear)
		PQclear(result);

	return result;
}

void Database_PostgreSQL::createTableIfNotExists(const std::string &table_name,
		const std::string &definition)
{
	std::string sql_check_table = "SELECT relname FROM pg_class WHERE relname='" +
		table_name + "';";
	PGresult *result = checkResults(PQexec(m_conn, sql_check_table.c_str()), false);

	// If table doesn't exist, create it
	if (!PQntuples(result)) {
		checkResults(PQexec(m_conn, definition.c_str()));
	}

	PQclear(result);
}

void Database_PostgreSQL::beginSave()
{
	verifyDatabase();
	checkResults(PQexec(m_conn, "BEGIN;"));
}

void Database_PostgreSQL::endSave()
{
	checkResults(PQexec(m_conn, "COMMIT;"));
}

void Database_PostgreSQL::rollback()
{
	checkResults(PQexec(m_conn, "ROLLBACK;"));
}

MapDatabasePostgreSQL::MapDatabasePostgreSQL(const std::string &connect_string)
    : Database_PostgreSQL(connect_string, ""),
      MapDatabase(),
      m_cache(g_settings->exists("map_block_cache_size") ?
             g_settings->getU64("map_block_cache_size") :
             100000)
{
    connectToDatabase();
}

void MapDatabasePostgreSQL::createDatabase()
{
    createTableIfNotExists("blocks",
        "CREATE TABLE blocks ("
            "posX SMALLINT NOT NULL,"
            "posY SMALLINT NOT NULL,"
            "posZ SMALLINT NOT NULL,"
            "data BYTEA,"
            "PRIMARY KEY (posX, posY, posZ)"
        ");"
    );

    // Check and migrate existing data
    createTableIfNotExists("schema_info",
        "CREATE TABLE schema_info (version INTEGER PRIMARY KEY)"
    );

    int current_version = 0;
    PGresult *version_res = PQexec(m_conn, "SELECT version FROM schema_info");
    if (PQresultStatus(version_res) == PGRES_TUPLES_OK && PQntuples(version_res) > 0) {
        current_version = atoi(PQgetvalue(version_res, 0, 0));
    }
    PQclear(version_res);

    if (current_version < 1) {
        // Begin transaction
        executeSQL("BEGIN");

        try {
            // Check for invalid coordinates
            PGresult *range_check = PQexec(m_conn,
                "SELECT COUNT(*) FROM blocks WHERE "
                "posX < -32768 OR posX > 32767 OR "
                "posY < -32768 OR posY > 32767 OR "
                "posZ < -32768 OR posZ > 32767");
            if (PQresultStatus(range_check) != PGRES_TUPLES_OK)
                throw DatabaseException("Failed to check coordinate ranges");

            int64_t invalid_count = pg_to_uint(range_check, 0, 0);
            PQclear(range_check);

            if (invalid_count > 0) {
                throw DatabaseException("Found blocks with coordinates outside smallint range");
            }

            // Convert columns to smallint if needed
            PGresult *col_check = PQexec(m_conn,
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name = 'blocks' AND column_name = 'posX'");
            std::string data_type = "smallint";
            if (PQresultStatus(col_check) == PGRES_TUPLES_OK && PQntuples(col_check) > 0) {
                data_type = PQgetvalue(col_check, 0, 0);
            }
            PQclear(col_check);

            if (data_type == "integer") {
                executeSQL("ALTER TABLE blocks "
                    "ALTER COLUMN posX TYPE SMALLINT, "
                    "ALTER COLUMN posY TYPE SMALLINT, "
                    "ALTER COLUMN posZ TYPE SMALLINT");
            }

            // Migrate block data
            PGresult *blocks_res = PQexec(m_conn, "SELECT posX, posY, posZ, data FROM blocks");
            if (PQresultStatus(blocks_res) != PGRES_TUPLES_OK) {
                PQclear(blocks_res);
                throw DatabaseException("Failed to fetch blocks for migration");
            }

            int num_blocks = PQntuples(blocks_res);
            for (int i = 0; i < num_blocks; i++) {
                int16_t posX = pg_to_smallint(blocks_res, i, 0);
                int16_t posY = pg_to_smallint(blocks_res, i, 1);
                int16_t posZ = pg_to_smallint(blocks_res, i, 2);
                std::string data = pg_to_string(blocks_res, i, 3);

                // Convert coordinates to network byte order
                int16_t newX = htons(posX);
                int16_t newY = htons(posY);
                int16_t newZ = htons(posZ);

                // Update posX, posY, posZ in binary format
                const char *pos_params[] = {
                    reinterpret_cast<const char*>(&newX),
                    reinterpret_cast<const char*>(&newY),
                    reinterpret_cast<const char*>(&newZ),
                    data.c_str(),
                    reinterpret_cast<const char*>(&posX),
                    reinterpret_cast<const char*>(&posY),
                    reinterpret_cast<const char*>(&posZ)
                };
                const int pos_lengths[] = {
                    sizeof(newX), sizeof(newY), sizeof(newZ),
                    static_cast<int>(data.size()),
                    sizeof(posX), sizeof(posY), sizeof(posZ)
                };
                const int pos_formats[] = {1, 1, 1, 1, 1, 1, 1};

                PGresult *update_res = PQexecParams(
                    m_conn,
                    "UPDATE blocks SET posX = $1, posY = $2, posZ = $3, data = $4 "
                    "WHERE posX = $5 AND posY = $6 AND posZ = $7",
                    7,
                    nullptr,
                    pos_params,
                    pos_lengths,
                    pos_formats,
                    1
                );
                if (PQresultStatus(update_res) != PGRES_COMMAND_OK) {
                    PQclear(update_res);
                    throw DatabaseException("Failed to update block data");
                }
                PQclear(update_res);
            }
            PQclear(blocks_res);

            // Update schema version
            executeSQL("INSERT INTO schema_info (version) VALUES (1) "
                       "ON CONFLICT (version) DO UPDATE SET version = EXCLUDED.version");

            // Commit transaction
            executeSQL("COMMIT");
        } catch (const DatabaseException &e) {
            executeSQL("ROLLBACK");
            throw;
        }
    }
	if (current_version < 2) { // Новая версия схемы
        executeSQL("ALTER TABLE blocks ADD COLUMN IF NOT EXISTS format_version SMALLINT DEFAULT 1");
        executeSQL("UPDATE schema_info SET version = 2");
    }

}

bool MapDatabasePostgreSQL::saveBlock(const v3s16 &pos, std::string_view data)
{
    std::string key = fmt::format("{},{},{}", pos.X, pos.Y, pos.Z);
    m_cache.put("map_blocks", key, std::string(data));  // Use 3 arguments
    // 1. Проверка размера данных
    if (data.size() > INT_MAX) {
        errorstream << "Block data size exceeds INT_MAX at position "
                   << pos << " (size: " << data.size() << ")" << std::endl;
        return false;
    }
    if (pos.X < -32768 || pos.X > 32767 || pos.Y < -32768 || pos.Y > 32767 || pos.Z < -32768 || pos.Z > 32767) {
    errorstream << "Invalid block position" << pos << std::endl;
    return false;
}

    // 2. Проверка пустых данных
    if (data.empty()) {
        errorstream << "Attempt to save empty block data at position "
                   << pos << std::endl;
        return false;
    }

    // 3. Преобразование координат с проверкой диапазона
    constexpr int16_t min_val = std::numeric_limits<int16_t>::min();
    constexpr int16_t max_val = std::numeric_limits<int16_t>::max();

    if (pos.X < -32768 || pos.X > 32767) {
    throw std::out_of_range("X coordinate out of smallint range");
}
int16_t x = static_cast<int16_t>(pos.X);


    if (pos.Y < -32768 || pos.Y > 32767) {
    throw std::out_of_range("Y coordinate out of smallint range");
}
int16_t y = static_cast<int16_t>(pos.Y);

    if (pos.Z < -32768 || pos.Z > 32767) {
    throw std::out_of_range("Z coordinate out of smallint range");
}
int16_t z = static_cast<int16_t>(pos.Z);

    // 4. Преобразование порядка байт для little-endian систем
    #ifdef WORDS_LITTLEENDIAN
    x = htons(x);
    y = htons(y);
    z = htons(z);
    #endif

    // 5. Подготовка параметров
    const char* args[] = { reinterpret_cast<const char*>(&x), reinterpret_cast<const char*>(&y), reinterpret_cast<const char*>(&z), data.data() };
    const int argLen[] = {
        sizeof(int16_t),
        sizeof(int16_t),
        sizeof(int16_t),
        static_cast<int>(data.size())
    };
    const int argFmt[] = { 1, 1, 1, 1 }; // Все параметры в бинарном формате

    try {
        // 6. Выполнение запроса
        execPrepared(
		"write_block",
		4,
		args,
		argLen,
		argFmt,
		1,      // resultFormat = 1 (бинарный)
		true    // clear = true
	);

        return true;
    } catch (const DatabaseException &e) {
        errorstream << "Failed to save block at " << pos
                   << ": " << e.what() << std::endl;
        return false;
    }
}

void MapDatabasePostgreSQL::loadBlock(const v3s16 &pos, std::string *block)
{
    std::string key = fmt::format("{},{},{}", pos.X, pos.Y, pos.Z);
    if (m_cache.get("map_blocks", key, *block)) {  // Use 3 arguments
        return;
    }

    int16_t x = static_cast<int16_t>(pos.X);
    int16_t y = static_cast<int16_t>(pos.Y);
    int16_t z = static_cast<int16_t>(pos.Z);

#ifdef WORDS_LITTLEENDIAN
    x = htons(x);
    y = htons(y);
    z = htons(z);
#endif

    const void *args[] = { &x, &y, &z };
    const int argLen[] = { sizeof(x), sizeof(y), sizeof(z) };
    const int argFmt[] = { 1, 1, 1 };

    PGresult *results = execPrepared("read_block",
        ARRLEN(args),
        reinterpret_cast<const char* const*>(args),
        argLen,
        argFmt,
        1,
		false);

    if (PQntuples(results))
        *block = std::string(PQgetvalue(results, 0, 0), PQgetlength(results, 0, 0)); // Бинарные данные
    else
        block->clear();

    PQclear(results);

    m_cache.put("map_blocks", key, *block);  // Use 3 arguments
}

bool MapDatabasePostgreSQL::deleteBlock(const v3s16 &pos)
{
    std::string key = fmt::format("{},{},{}", pos.X, pos.Y, pos.Z);
    m_cache.remove("map_blocks", key);  // Use 2 arguments
    verifyDatabase();

    // Используем int16_t вместо s32
    int16_t x = htons(static_cast<int16_t>(pos.X));
    int16_t y = htons(static_cast<int16_t>(pos.Y));
    int16_t z = htons(static_cast<int16_t>(pos.Z));
	#ifdef WORDS_LITTLEENDIAN
	x = htons(x);
	y = htons(y);
	z = htons(z);
	#endif


    const void *args[] = { &x, &y, &z };
    const int argLen[] = { sizeof(x), sizeof(y), sizeof(z) };
    const int argFmt[] = { 1, 1, 1 };

    execPrepared(
        "delete_block",
        3,
        reinterpret_cast<const char* const*>(args),
        argLen,
        argFmt,
        true,
		1 // binary
    );

    return true;
}

void MapDatabasePostgreSQL::listAllLoadableBlocks(std::vector<v3s16> &dst)
{
	verifyDatabase();

	PGresult *results = execPrepared("list_all_loadable_blocks", 0, nullptr, nullptr, nullptr, 0, false);

	int numrows = PQntuples(results);

	for (int row = 0; row < numrows; ++row)
		dst.push_back(pg_to_v3s16(results, row, 0));

	PQclear(results);
}

void MapDatabasePostgreSQL::initStatements()
{
    // Prepare SQL statements for map block operations
    prepareStatement("write_block",
        "INSERT INTO blocks (posX, posY, posZ, data) "
        "VALUES ($1::smallint, $2::smallint, $3::smallint, $4::bytea) "
        "ON CONFLICT (posX, posY, posZ) DO UPDATE SET data = EXCLUDED.data");

    prepareStatement("read_block",
        "SELECT data FROM blocks "
        "WHERE posX = $1::smallint AND posY = $2::smallint AND posZ = $3::smallint");

    prepareStatement("delete_block",
        "DELETE FROM blocks "
        "WHERE posX = $1::smallint AND posY = $2::smallint AND posZ = $3::smallint");

    prepareStatement("list_all_loadable_blocks",
        "SELECT posX, posY, posZ FROM blocks");

    infostream << "PostgreSQL: Map Database prepared statements initialized." << std::endl;
}

/*
 * Player Database
 */
PlayerDatabasePostgreSQL::PlayerDatabasePostgreSQL(const std::string &connect_string):
	Database_PostgreSQL(connect_string, "_player"),
	PlayerDatabase()
{
	connectToDatabase();
}


void PlayerDatabasePostgreSQL::createDatabase()
{
    // 1. Основная таблица игроков
    createTableIfNotExists("player",
        "CREATE TABLE player ("
            "name TEXT PRIMARY KEY, "                  // Используем TEXT вместо VARCHAR
            "pitch SMALLINT NOT NULL, "                // Оптимизация числовых полей
            "yaw SMALLINT NOT NULL, "
            "posX SMALLINT NOT NULL, "
            "posY SMALLINT NOT NULL, "
            "posZ SMALLINT NOT NULL, "
            "hp SMALLINT NOT NULL CHECK (hp BETWEEN 0 AND 32767), "
            "breath SMALLINT NOT NULL CHECK (breath BETWEEN 0 AND 32767), "
            "creation_date TIMESTAMPTZ NOT NULL DEFAULT NOW(), "  // Исправление типа времени
            "modification_date TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        ");"
    );

    // 2. Инвентари
    createTableIfNotExists("player_inventories",
        "CREATE TABLE player_inventories ("
            "player TEXT REFERENCES player(name) ON DELETE CASCADE, "
            "inv_id SMALLINT NOT NULL, "               // Уменьшаем размерность
            "inv_width SMALLINT NOT NULL, "
            "inv_name TEXT NOT NULL DEFAULT '', "
            "inv_size SMALLINT NOT NULL, "
            "PRIMARY KEY(player, inv_id)"
        ");"
    );

    // 3. Предметы в инвентаре
    createTableIfNotExists("player_inventory_items",
        "CREATE TABLE player_inventory_items ("
            "player TEXT REFERENCES player(name) ON DELETE CASCADE, "
            "inv_id SMALLINT NOT NULL, "
            "slot_id SMALLINT NOT NULL, "              // Оптимизация
            "item TEXT NOT NULL DEFAULT '', "
            "PRIMARY KEY(player, inv_id, slot_id)"
        ");"
    );

    // 4. Метаданные
    createTableIfNotExists("player_metadata",
        "CREATE TABLE player_metadata ("
            "player TEXT REFERENCES player(name) ON DELETE CASCADE, "
            "attr TEXT NOT NULL, "                      // Используем TEXT вместо VARCHAR
            "value TEXT, "
            "PRIMARY KEY(player, attr)"
        ");"
    );

    // 5. Дополнительные индексы
    executeSQL(
        "CREATE INDEX IF NOT EXISTS idx_player_modified ON player(modification_date);"
        "CREATE INDEX IF NOT EXISTS idx_metadata_attr ON player_metadata(attr);"
    );

    infostream << "PostgreSQL: Player Database initialized successfully." << std::endl;
}

void PlayerDatabasePostgreSQL::initStatements()
{
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
    prepareStatement("remove_player",
        "DELETE FROM player WHERE name = $1");

    prepareStatement("load_player_list",
        "SELECT name FROM player");

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
}

bool PlayerDatabasePostgreSQL::playerDataExists(const std::string &playername)
{
	const char* values[] = { playername.c_str() };
	PGresult *results = execPrepared("load_player", 1, values, nullptr, nullptr, 0, false);

	bool res = (PQntuples(results) > 0);
	PQclear(results);
	return res;
}

void PlayerDatabasePostgreSQL::savePlayer(RemotePlayer *player)
{
	PlayerSAO* sao = player->getPlayerSAO();
	if (!sao)
		return;

	verifyDatabase();

	v3f pos = sao->getBasePosition();
	std::string pitch = ftos(sao->getLookPitch());
	std::string yaw = ftos(sao->getRotation().Y);
	std::string posx = ftos(pos.X);
	std::string posy = ftos(pos.Y);
	std::string posz = ftos(pos.Z);
	std::string hp = itos(sao->getHP());
	std::string breath = itos(sao->getBreath());
	const char *values[] = {
		player->getName().c_str(),
		pitch.c_str(),
		yaw.c_str(),
		posx.c_str(), posy.c_str(), posz.c_str(),
		hp.c_str(),
		breath.c_str()
	};

	const char* rmvalues[] = { player->getName().c_str() };
	Database_PostgreSQL::beginSave();

	execPrepared("save_player", 8, values, nullptr, nullptr, 0, true);


	// Write player inventories
	execPrepared("remove_player_inventories", 1, rmvalues, nullptr, nullptr, 0, true);

	execPrepared("remove_player_inventory_items", 1, rmvalues, nullptr, nullptr, 0, true);

	const auto &inventory_lists = sao->getInventory()->getLists();
	std::ostringstream oss;
	for (u16 i = 0; i < inventory_lists.size(); i++) {
		const InventoryList* list = inventory_lists[i];
		const std::string &name = list->getName();
		std::string width = itos(list->getWidth()),
			inv_id = itos(i), lsize = itos(list->getSize());

		const char* inv_values[] = {
			player->getName().c_str(),
			inv_id.c_str(),
			width.c_str(),
			name.c_str(),
			lsize.c_str()
		};
		execPrepared("add_player_inventory", 5, inv_values, nullptr, nullptr, 1, true);

		for (u32 j = 0; j < list->getSize(); j++) {
			oss.str("");
			oss.clear();
			list->getItem(j).serialize(oss);
			std::string itemStr = oss.str(), slotId = itos(j);

			const char* invitem_values[] = {
				player->getName().c_str(),
				inv_id.c_str(),
				slotId.c_str(),
				itemStr.c_str()
			};
			execPrepared("add_player_inventory_item", 4, invitem_values, nullptr, nullptr, 1, true);
		}
	}

	execPrepared("remove_player_metadata", 1, rmvalues, nullptr, nullptr, 0, true);
	const StringMap &attrs = sao->getMeta().getStrings();
	for (const auto &attr : attrs) {
		const char *meta_values[] = {
			player->getName().c_str(),
			attr.first.c_str(),
			attr.second.c_str()
		};
		execPrepared("save_player_metadata", 3, meta_values, nullptr, nullptr, 1, true);
	}
	Database_PostgreSQL::endSave();

	player->onSuccessfulSave();
}

bool PlayerDatabasePostgreSQL::loadPlayer(RemotePlayer *player, PlayerSAO *sao)
{
	sanity_check(sao);
	verifyDatabase();

	const char *values[] = { player->getName().c_str() };
	PGresult *results = execPrepared("load_player", 1, values, nullptr, nullptr, 0, false); // clear=false
    if (!PQntuples(results)) {
        PQclear(results); // Explicit clear
        return false;
    }

	sao->setLookPitch(pg_to_float(results, 0, 0));
	sao->setRotation(v3f(0, pg_to_float(results, 0, 1), 0));
	sao->setBasePosition(v3f(
		pg_to_float(results, 0, 2),
		pg_to_float(results, 0, 3),
		pg_to_float(results, 0, 4))
	);
	sao->setHPRaw((u16) pg_to_int(results, 0, 5));
	sao->setBreath((u16) pg_to_int(results, 0, 6), false);

	PQclear(results);

	// Load inventory
	results = execPrepared("load_player_inventories", 1, values, nullptr, nullptr, 1, true);

	int resultCount = PQntuples(results);

	for (int row = 0; row < resultCount; ++row) {
		InventoryList* invList = player->inventory.
			addList(PQgetvalue(results, row, 2), pg_to_uint(results, row, 3));
		invList->setWidth(pg_to_uint(results, row, 1));

		u32 invId = pg_to_uint(results, row, 0);
		std::string invIdStr = itos(invId);

		const char* values2[] = {
			player->getName().c_str(),
			invIdStr.c_str()
		};
		PGresult *results2 = execPrepared(
			"load_player_inventory_items",
			2,
			values2,
			nullptr,
			nullptr,
			1, //binary
			true
);

		int resultCount2 = PQntuples(results2);
		for (int row2 = 0; row2 < resultCount2; row2++) {
			const std::string itemStr = PQgetvalue(results2, row2, 1);
			if (itemStr.length() > 0) {
				ItemStack stack;
				stack.deSerialize(itemStr);
				invList->changeItem(pg_to_uint(results2, row2, 0), stack);
			}
		}
		PQclear(results2);
	}

	PQclear(results);

	results = execPrepared("load_player_metadata", 1, values, nullptr, nullptr, 1, true);

	int numrows = PQntuples(results);
	for (int row = 0; row < numrows; row++) {
		sao->getMeta().setString(PQgetvalue(results, row, 0), PQgetvalue(results, row, 1));
	}
	sao->getMeta().setModified(false);

	PQclear(results);

	return true;
}

bool PlayerDatabasePostgreSQL::removePlayer(const std::string &name)
{
	if (!playerDataExists(name))
		return false;

	verifyDatabase();

	const char *values[] = { name.c_str() };
	execPrepared("remove_player", 1, values, nullptr, nullptr, 0, true);


	return true;
}

void PlayerDatabasePostgreSQL::listPlayers(std::vector<std::string> &res)
{
	verifyDatabase();

	PGresult *results = execPrepared("load_player_list", 0, nullptr, nullptr, nullptr, 0, false);

	int numrows = PQntuples(results);
	for (int row = 0; row < numrows; row++)
		res.emplace_back(PQgetvalue(results, row, 0));

	PQclear(results);
}

AuthDatabasePostgreSQL::AuthDatabasePostgreSQL(const std::string &connect_string) :
	Database_PostgreSQL(connect_string, "_auth"),
	AuthDatabase()
{
	connectToDatabase();
}

void AuthDatabasePostgreSQL::createDatabase()
{
	createTableIfNotExists("auth",
		"CREATE TABLE auth ("
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

void AuthDatabasePostgreSQL::initStatements()
{
	prepareStatement("auth_read", "SELECT id, name, password, last_login FROM auth WHERE name = $1");
	prepareStatement("auth_write", "UPDATE auth SET name = $1, password = $2, last_login = $3 WHERE id = $4");
	prepareStatement("auth_create", "INSERT INTO auth (name, password, last_login) VALUES ($1, $2, $3) RETURNING id");
	prepareStatement("auth_delete", "DELETE FROM auth WHERE name = $1");
	prepareStatement("auth_list_names", "SELECT name FROM auth ORDER BY name DESC");
	prepareStatement("auth_read_privs", "SELECT privilege FROM user_privileges WHERE id = $1");
	prepareStatement("auth_write_privs", "INSERT INTO user_privileges (id, privilege) VALUES ($1, $2)");
	prepareStatement("auth_delete_privs", "DELETE FROM user_privileges WHERE id = $1");
}

bool AuthDatabasePostgreSQL::getAuth(const std::string &name, AuthEntry &res)
{
	verifyDatabase();

	const char *values[] = { name.c_str() };
	PGresult *result = execPrepared("auth_read", 1, values, nullptr, nullptr, 0, false);
	int numrows = PQntuples(result);
	if (numrows == 0) {
		PQclear(result);
		return false;
	}

	res.id = pg_to_uint(result, 0, 0);
	res.name = pg_to_string(result, 0, 1);
	res.password = pg_to_string(result, 0, 2);
	res.last_login = pg_to_int(result, 0, 3);

	PQclear(result);

	std::string playerIdStr = itos(res.id);
	const char *privsValues[] = { playerIdStr.c_str() };
	PGresult *results = execPrepared("auth_read_privs", 1, privsValues, nullptr, nullptr, 0, false);
	numrows = PQntuples(results);
	for (int row = 0; row < numrows; row++)
		res.privileges.emplace_back(PQgetvalue(results, row, 0));

	PQclear(results);

	return true;
}

bool AuthDatabasePostgreSQL::saveAuth(const AuthEntry &authEntry)
{
	verifyDatabase();

	Database_PostgreSQL::beginSave();

	std::string lastLoginStr = itos(authEntry.last_login);
	std::string idStr = itos(authEntry.id);
	const char *values[] = {
		authEntry.name.c_str() ,
		authEntry.password.c_str(),
		lastLoginStr.c_str(),
		idStr.c_str(),
	};
	execPrepared("auth_write", 4, values, nullptr, nullptr, 0, true);

	writePrivileges(authEntry);

	Database_PostgreSQL::endSave();
	return true;
}

bool AuthDatabasePostgreSQL::createAuth(AuthEntry &authEntry)
{
	verifyDatabase();

	std::string lastLoginStr = itos(authEntry.last_login);
	const char *values[] = {
		authEntry.name.c_str() ,
		authEntry.password.c_str(),
		lastLoginStr.c_str()
	};

    Database_PostgreSQL::beginSave();
    infostream << "Starting auth migration for: " << authEntry.name << std::endl;

    PGresult *result = execPrepared("auth_create", 3, values, nullptr, nullptr, 0, false);

    // Добавленные проверки
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        errorstream << "Query failed: " << PQresultErrorMessage(result) << std::endl;
        PQclear(result);
        Database_PostgreSQL::rollback();
        return false;
    }

    if (PQntuples(result) != 1 || PQnfields(result) < 1) {
        errorstream << "Invalid result structure" << std::endl;
        PQclear(result);
        Database_PostgreSQL::rollback();
        return false;
    }

    if (PQgetisnull(result, 0, 0)) {
        errorstream << "NULL ID detected" << std::endl;
        PQclear(result);
        Database_PostgreSQL::rollback();
        return false;
    }

    authEntry.id = pg_to_uint(result, 0, 0);
    PQclear(result);

	writePrivileges(authEntry);

	Database_PostgreSQL::endSave();
	return true;
}

bool AuthDatabasePostgreSQL::deleteAuth(const std::string &name)
{
	verifyDatabase();

	const char *values[] = { name.c_str() };
	execPrepared("auth_delete", 1, values, nullptr, nullptr, 0, true);

	// privileges deleted by foreign key on delete cascade
	return true;
}

void AuthDatabasePostgreSQL::listNames(std::vector<std::string> &res)
{
	verifyDatabase();
	PGresult *results = execPrepared("auth_list_names", 0, nullptr, nullptr, nullptr, 0, false);


	int numrows = PQntuples(results);

	for (int row = 0; row < numrows; ++row)
		res.emplace_back(PQgetvalue(results, row, 0));

	PQclear(results);
}

void AuthDatabasePostgreSQL::reload()
{
	// noop for PgSQL
}

void AuthDatabasePostgreSQL::writePrivileges(const AuthEntry &authEntry)
{
	std::string authIdStr = itos(authEntry.id);
	const char *values[] = { authIdStr.c_str() };
	execPrepared("auth_delete_privs", 1, values, nullptr, nullptr, 0, true);

	for (const std::string &privilege : authEntry.privileges) {
		const char *values[] = { authIdStr.c_str(), privilege.c_str() };
		execPrepared("auth_write_privs", 2, values, nullptr, nullptr, 0, true);
	}
}

ModStorageDatabasePostgreSQL::ModStorageDatabasePostgreSQL(const std::string &connect_string)
    : Database_PostgreSQL(connect_string, "_mod_storage"),
      ModStorageDatabase(),
      m_cache(g_settings->exists("mod_storage_cache_size")
		? g_settings->getU64("mod_storage_cache_size")
		: 100000),
      m_running(true),
      m_writer_thread(&ModStorageDatabasePostgreSQL::writerThread, this)
{
    connectToDatabase();
}

ModStorageDatabasePostgreSQL::~ModStorageDatabasePostgreSQL() {
    m_running = false;
    m_write_cv.notify_all();

    // Очистка очереди
    std::queue<std::tuple<std::string, std::string, std::string>> empty;
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        m_write_queue.swap(empty);
    }

    if (m_writer_thread.joinable())
        m_writer_thread.join();
}

// Новые методы для асинхронной записи:
void ModStorageDatabasePostgreSQL::writerThread()
{
    while (m_running) {
        std::unique_lock<std::mutex> lock(m_write_mutex);
        // Add timeout to prevent hanging on shutdown
        m_write_cv.wait_for(lock, std::chrono::seconds(1), [this] {
            return !m_write_queue.empty() || !m_running;
        });

        if (!m_running) break;

        try {
            flushWriteQueue();
        } catch (const std::exception &e) {
            errorstream << "Database writer error: " << e.what() << std::endl;
            Database_PostgreSQL::rollback();
        }
    }
    // Flush remaining items on shutdown
    flushWriteQueue();
}

void ModStorageDatabasePostgreSQL::flushWriteQueue() {
    if (!m_conn || PQstatus(m_conn) != CONNECTION_OK) {
        errorstream << "Connection lost during flush" << std::endl;
        return;
    }

    Database_PostgreSQL::beginSave();

    try {
        std::vector<std::tuple<std::string, std::string, std::string>> batch;
        {
            std::lock_guard<std::mutex> lock(m_write_mutex);
            while (!m_write_queue.empty() && batch.size() < 1000) {
                batch.push_back(m_write_queue.front());
                m_write_queue.pop();
            }
        }

        if (batch.empty()) {
            Database_PostgreSQL::rollback();
            return;
        }

        // Формируем SQL запрос с динамическими параметрами
        std::vector<const char*> params;
        std::vector<std::string> placeholders;
        int param_counter = 1;

        for (const auto& entry : batch) {
            placeholders.push_back(fmt::format("(${}, ${}, ${})",
                param_counter, param_counter+1, param_counter+2));
            param_counter += 3;

            params.push_back(std::get<0>(entry).c_str()); // modname
            params.push_back(std::get<1>(entry).c_str()); // key
            params.push_back(std::get<2>(entry).c_str()); // value
        }

        // 1. Формируем основной запрос
        std::string sql = fmt::format(
            "INSERT INTO mod_storage (modname, key, value) VALUES {}\n"
            "ON CONFLICT (modname, key) DO UPDATE SET value = EXCLUDED.value",
            fmt::join(placeholders, ", ")
        );

        // 2. Выполняем запрос напрямую
        PGresult* res = PQexecParams(
            m_conn,
            sql.c_str(),
            params.size(),
            nullptr, // типы параметров не указаны (текст по умолчанию)
            params.data(),
            nullptr, // длины параметров (текст определяется по нуль-терминатору)
            nullptr, // форматы параметров (0 = текст)
            0 // формат результата (0 = текст)
        );

        // 3. Проверяем результат
        ExecStatusType status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK) {
            std::string err = PQresultErrorMessage(res);
            PQclear(res);
            throw DatabaseException("Batch insert failed: " + err);
        }
        PQclear(res);

        Database_PostgreSQL::endSave();

    } catch (const DatabaseException &e) {
        Database_PostgreSQL::rollback();
        errorstream << "Failed to flush write queue: " << e.what() << std::endl;
        // Реинициализация соединения при ошибке
        reconnectDatabase();
    }
}

void ModStorageDatabasePostgreSQL::createDatabase()
{
	createTableIfNotExists("mod_storage",
		"CREATE TABLE mod_storage ("
			"modname TEXT NOT NULL,"
			"key BYTEA NOT NULL,"
			"value BYTEA NOT NULL,"
			"PRIMARY KEY (modname, key)"
		");");

	infostream << "PostgreSQL: Mod Storage Database was initialized." << std::endl;
}

void ModStorageDatabasePostgreSQL::initStatements()
{
    // Основные операции
    prepareStatement("get_all",
        "SELECT key, value FROM mod_storage WHERE modname = $1");

    prepareStatement("get_all_keys",
        "SELECT key FROM mod_storage WHERE modname = $1");

    prepareStatement("get",
        "SELECT value FROM mod_storage WHERE modname = $1 AND key = $2::bytea");

    prepareStatement("has",
        "SELECT EXISTS (SELECT 1 FROM mod_storage WHERE modname = $1 AND key = $2::bytea)");

    // Унифицированный UPSERT
    prepareStatement("set",
        "INSERT INTO mod_storage (modname, key, value) VALUES ($1, $2::bytea, $3::bytea) "
        "ON CONFLICT (modname, key) DO UPDATE SET value = EXCLUDED.value");

    // Пакетная вставка
    prepareStatement("batch_set",
        "INSERT INTO mod_storage (modname, key, value) VALUES %s "
        "ON CONFLICT (modname, key) DO UPDATE SET value = EXCLUDED.value");

    // Удаление данных
    prepareStatement("remove",
        "DELETE FROM mod_storage WHERE modname = $1 AND key = $2::bytea");

    prepareStatement("remove_all",
        "DELETE FROM mod_storage WHERE modname = $1");

    prepareStatement("list",
        "SELECT DISTINCT modname FROM mod_storage");
}

void ModStorageDatabasePostgreSQL::getModEntries(const std::string &modname, StringMap *storage) {

    const char* args[] = { modname.c_str() };
	const int argLen[] = { -1 }; // Добавляем
    const int argFmt[] = { 0 };
    PGresult *results = execPrepared("get_all", ARRLEN(args),
        args, argLen, argFmt, false);

	int numrows = PQntuples(results);

	for (int row = 0; row < numrows; ++row)
		(*storage)[pg_to_string(results, row, 0)] = pg_to_string(results, row, 1);

	PQclear(results);
}

void ModStorageDatabasePostgreSQL::getModKeys(const std::string &modname,
		std::vector<std::string> *storage)
{
	verifyDatabase();

	const void *args[] = { modname.c_str() };
	const int argLen[] = { -1 };
	const int argFmt[] = { 0 };
	PGresult *results = execPrepared("get_all_keys", ARRLEN(args),
    reinterpret_cast<const char* const*>(args), argLen, argFmt, false);

	int numrows = PQntuples(results);

	storage->reserve(storage->size() + numrows);
	for (int row = 0; row < numrows; ++row)
		storage->push_back(pg_to_string(results, row, 0));

	PQclear(results);
}

bool ModStorageDatabasePostgreSQL::getModEntry(
    const std::string &modname, const std::string &key, std::string *value)
{
    if (m_cache.get(modname, key, *value))
        return !value->empty(); // Возвращаем false для пустого негативного кэша

    verifyDatabase();

    const char *args[] = { modname.c_str(), key.c_str() };
    const int argLen[] = { -1, (int)MYMIN(key.size(), INT_MAX) };
    const int argFmt[] = { 0, 1 };
    PGresult *results = execPrepared("get", ARRLEN(args), args, argLen, argFmt, 0, false);

    bool found = PQntuples(results) > 0;
    if (found) {
        *value = pg_to_string(results, 0, 0);
        m_cache.put(modname, key, *value);
    } else {
        m_cache.put(modname, key, ""); // Негативный кэш
    }
    PQclear(results); // Важно: очищаем в любом случае

    return found;
}

bool ModStorageDatabasePostgreSQL::hasModEntry(const std::string &modname,
		const std::string &key)
{
	verifyDatabase();

	const char *args[] = { modname.c_str(), key.c_str() };
	const int argLen[] = { -1, (int)MYMIN(key.size(), INT_MAX) };
	const int argFmt[] = { 0, 1 };
	PGresult *results = execPrepared("has", ARRLEN(args),
    reinterpret_cast<const char* const*>(args), argLen, argFmt, false);

	int numrows = PQntuples(results);
	bool found = numrows > 0;

	PQclear(results);

	return found;
}

bool ModStorageDatabasePostgreSQL::setModEntry(
    const std::string &modname, const std::string &key, std::string_view value)
{
    // Сначала обновляем кэш
    m_cache.put(modname, key, std::string(value));

    // Ставим в очередь на асинхронную запись
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        m_write_queue.emplace(modname, key, std::string(value));
    }
    m_write_cv.notify_one();

    return true;
}

bool ModStorageDatabasePostgreSQL::removeModEntry(
    const std::string &modname, const std::string &key)
{
	 {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        std::queue<std::tuple<std::string, std::string, std::string>> new_queue;
        while (!m_write_queue.empty()) {
            auto entry = m_write_queue.front();
            m_write_queue.pop();
            if (std::get<0>(entry) != modname ||
                std::get<1>(entry) != key) {
                new_queue.push(entry);
            }
        }
        m_write_queue.swap(new_queue);
    }
    // Очищаем кэш
    m_cache.remove(modname, key);

    // Синхронное удаление из БД
    verifyDatabase();

    const void *args[] = { modname.c_str(), key.c_str() };
    const int argLen[] = { -1, (int)MYMIN(key.size(), INT_MAX) };
    const int argFmt[] = { 0, 1 };
    PGresult *results = execPrepared("remove", ARRLEN(args),
    reinterpret_cast<const char* const*>(args), argLen, argFmt, false);

    int affected = atoi(PQcmdTuples(results));
    PQclear(results);

    return affected > 0;
}


bool ModStorageDatabasePostgreSQL::removeModEntries(const std::string &modname)
{
    // Очищаем весь кэш для мода
    m_cache.purgeMod(modname);

    // Синхронное удаление из БД
    verifyDatabase();

    const void *args[] = { modname.c_str() };
    const int argLen[] = { -1 };
    const int argFmt[] = { 0 };
    PGresult *results = execPrepared("remove_all", ARRLEN(args),
    reinterpret_cast<const char* const*>(args), argLen, argFmt, false);

    int affected = atoi(PQcmdTuples(results));
    PQclear(results);

    return affected > 0;
}

void ModStorageDatabasePostgreSQL::listMods(std::vector<std::string> *res)
{
	verifyDatabase();

	PGresult *results = execPrepared("list", 0, nullptr, nullptr, nullptr, 0, false);

	int numrows = PQntuples(results);

	for (int row = 0; row < numrows; ++row)
		res->push_back(pg_to_string(results, row, 0));

	PQclear(results);
}

Database_PostgreSQL::~Database_PostgreSQL() {
    if (m_conn) {
        infostream << "Closing PostgreSQL connection: " << m_conn << std::endl;
        PQfinish(m_conn);
        m_conn = nullptr;
    }
}


#endif // USE_POSTGRESQL
