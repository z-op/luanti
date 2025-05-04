// Luanti fast asyncronous postgresql connector
// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (C) 2013 celeron55, Perttu Ahola <celeron55@gmail.com>
// Copyright (C) 2025 Evgeniy Pudikov <evgenuel@gmail.com>
#pragma once
#ifndef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY
#endif
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <thread>
#include <tuple>
#include <fmt/format.h>
#include <fmt/core.h>
#include <string>
#include <libpq-fe.h>
#include "database.h"
#include "util/basic_macros.h"
#include <memory>
#include <unordered_map>
#include <list>
#include <functional>
#include <unordered_set>

class PGConnWrapper {
	PGconn * m_conn;
	public: PGConnWrapper(const std::string & conninfo): m_conn(PQconnectdb(
			conninfo.c_str())) {}
		~PGConnWrapper() {
			if(m_conn) PQfinish(m_conn);
		}
	operator PGconn * () {
		return m_conn;
	}
};
struct KeyHash {
	size_t operator()(const std::pair < std::string, std::string > & k) const {
		size_t h1 = std::hash < std::string > {}(k.first);
		size_t h2 = std::hash < std::string > {}(k.second);
		return h1 ^ (h2 << 1);
	}
};
class PGResultWrapper {
	PGresult * m_res;
	public: explicit PGResultWrapper(PGresult * res): m_res(res) {}
		~PGResultWrapper() {
			if(m_res) PQclear(m_res);
		}
	PGresult * get() const {
		return m_res;
	}
};

/**
 * LRU кэш для хранения данных mod_storage
 * Особенности:
 * - Потокобезопасный доступ
 * - Автоматическая инвалидация по модам
 * - Негативное кэширование
 */

class CacheManager {
	public: using KeyType = std::pair < std::string,
	std::string > ;
	explicit CacheManager(size_t size);
	bool get(const std::string & modname,
		const std::string & key, std::string & value);
	void put(const std::string & modname,
		const std::string & key,
			const std::string & value);
	void remove(const std::string & modname,
		const std::string & key);
	void purgeMod(const std::string & modname);
	private: struct CacheEntry {
		std::string value;
		std::list < KeyType > ::iterator lru_it;
	};
	std::unordered_map < KeyType,
	CacheEntry,
	KeyHash > cache;
	std::list < KeyType > lru_queue;
	std::mutex cache_mutex;
	size_t max_size;
};
class Database_PostgreSQL: public Database {
	public:
	virtual~Database_PostgreSQL();
	bool checkColumnType(
    const std::string& table_name,
    const std::string& column_name,
    const std::string& expected_type
    );
	static inline int16_t pg_to_smallint(PGresult * res, int row, int col) {
		return static_cast < int16_t > (atoi(PQgetvalue(res, row, col)));
	}
	inline v3s16 pg_to_v3s16(PGresult * res, int row, int col) {
		int16_t x = pg_to_smallint(res, row, col);
		int16_t y = pg_to_smallint(res, row, col + 1);
		int16_t z = pg_to_smallint(res, row, col + 2);
		// Convert from network byte order if needed
		#ifdef WORDS_LITTLEENDIAN
		x = ntohs(x);
		y = ntohs(y);
		z = ntohs(z);
		#endif
		return v3s16(x, y, z);
	}
	Database_PostgreSQL(const std::string & connect_string,
		const char * type);
	int getPGVersion() const {
		return m_pgversion;
	}
	void beginSave() override;
	void endSave() override;
	void rollback();
	bool initialized() const override;
	void verifyDatabase();
	void reconnectDatabase();
	protected:
		// Connection management
		void connectToDatabase();
	// Helpers
	void createTableIfNotExists(const std::string & table_name,
		const std::string & definition);
	void prepareStatement(const char * name,
		const char * sql);
	// Execution methods
	PGresult * execPrepared(const char * stmtName,
		int paramsNumber,
		const char *
			const * params,
			const int * paramsLengths = nullptr,
				const int * paramsFormats = nullptr,
					int resultFormat = 0,
					bool clear = true);
	// Conversion helpers
	int pg_to_int(PGresult * res, int row, int col) {
		return atoi(PQgetvalue(res, row, col));
	}
	u32 pg_to_uint(PGresult * res, int row, int col) {
		return (u32) atoi(PQgetvalue(res, row, col));
	}
	float pg_to_float(PGresult * res, int row, int col) {
		return (float) atof(PQgetvalue(res, row, col));
	}
	std::string pg_to_string(PGresult * res, int row, int col) {
		return std::string(PQgetvalue(res, row, col), PQgetlength(res, row,
			col));
	}
	void executeSQL(const std::string & sql) {
		checkResults(PQexec(m_conn, sql.c_str()));
	}
	// Database info
	PGconn * m_conn = nullptr;
	int m_pgversion = 0;
	private: void ping();
	std::string m_connect_string;
	// Disable copy constructor and assignment operator
	Database_PostgreSQL(const Database_PostgreSQL & ) = delete;
	Database_PostgreSQL & operator = (const Database_PostgreSQL & ) =
	delete;
	protected: virtual void createDatabase() = 0;
	virtual void initStatements() = 0;
	PGresult * checkResults(PGresult * res, bool clear = true);
};
#define PARENT_CLASS_FUNCS\
void beginSave() override {
	Database_PostgreSQL::beginSave();
}\
void endSave() override {
	Database_PostgreSQL::endSave();
}\
void verifyDatabase() {
	Database_PostgreSQL::verifyDatabase();
}
class MapDatabasePostgreSQL: private Database_PostgreSQL, public MapDatabase {
	public: MapDatabasePostgreSQL(const std::string & connect_string);
	~MapDatabasePostgreSQL() override =
	default;
	bool saveBlock(const v3s16 & pos, std::string_view data) override;
	void loadBlock(const v3s16 & pos, std::string * block) override;
	bool deleteBlock(const v3s16 & pos) override;
	void listAllLoadableBlocks(std::vector < v3s16 > & dst) override;
	PARENT_CLASS_FUNCS
	protected: void createDatabase() override;
	void initStatements() override;
	private: std::string posToString(const v3s16 &
	pos) const; // Declaration only
	CacheManager m_cache;
};
class ModStorageDatabasePostgreSQL: private Database_PostgreSQL,
	public ModStorageDatabase {
		public: ModStorageDatabasePostgreSQL(const std::string &
		connect_string);
		~ModStorageDatabasePostgreSQL() override;
		void getModEntries(const std::string & modname, StringMap *
		storage) override;
		void getModKeys(const std::string & modname, std::vector < std::string >
			* storage) override;
		bool getModEntry(const std::string & modname,
			const std::string & key, std::string * value) override;
		bool hasModEntry(const std::string & modname,
			const std::string & key) override;
		bool setModEntry(const std::string & modname,
			const std::string & key, std::string_view value) override;
		bool removeModEntry(const std::string & modname,
			const std::string & key) override;
		bool removeModEntries(const std::string & modname) override;
		void listMods(std::vector < std::string > * res) override;
		PARENT_CLASS_FUNCS
		protected: void createDatabase() override;
		void initStatements() override;
		private: CacheManager m_cache;
		std::mutex m_write_mutex;
		std::condition_variable m_write_cv;
		std::queue < std::tuple < std::string,
		std::string,
		std::string >> m_write_queue;
		std::atomic < bool > m_running {
			true
		};
		std::thread m_writer_thread;
		void writerThread();
		void flushWriteQueue();
	};
class PlayerDatabasePostgreSQL: private Database_PostgreSQL,
	public PlayerDatabase {
		public: PlayerDatabasePostgreSQL(const std::string & connect_string);
		virtual~PlayerDatabasePostgreSQL() =
		default;
		void savePlayer(RemotePlayer * player);
		bool loadPlayer(RemotePlayer * player, PlayerSAO * sao);
		bool removePlayer(const std::string & name);
		void listPlayers(std::vector < std::string > & res);
		PARENT_CLASS_FUNCS
		protected: void createDatabase() override;
		void initStatements() override;
		private: bool playerDataExists(const std::string & playername);
	};
class AuthDatabasePostgreSQL: private Database_PostgreSQL, public AuthDatabase {
	public: AuthDatabasePostgreSQL(const std::string & connect_string);
	virtual~AuthDatabasePostgreSQL() =
	default;
	virtual bool getAuth(const std::string & name, AuthEntry & res);
	virtual bool saveAuth(const AuthEntry & authEntry);
	virtual bool createAuth(AuthEntry & authEntry);
	virtual bool deleteAuth(const std::string & name);
	virtual void listNames(std::vector < std::string > & res);
	virtual void reload();
	PARENT_CLASS_FUNCS
	protected: void createDatabase() override;
	void initStatements() override;
	private: virtual void writePrivileges(const AuthEntry & authEntry);
};
#undef PARENT_CLASS_FUNCS
