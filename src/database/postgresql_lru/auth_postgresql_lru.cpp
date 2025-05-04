#include "auth_database_postgresql.h"
#include "util/sha.h"
#include "exceptions.h"
#include "irrlichttypes.h"
#include <fmt/format.h>
#include <array>

AuthDatabasePostgreSQL::AuthDatabasePostgreSQL(const std::string& connect_string)
    : BaseDatabasePostgreSQL(connect_string, "auth", 10000) {}

void AuthDatabasePostgreSQL::createDatabase() {
    executeQuery(R"(
        CREATE TABLE IF NOT EXISTS accounts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(32) UNIQUE NOT NULL,
            password CHAR(64) NOT NULL, -- SHA-256 hex
            salt CHAR(32) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            last_login TIMESTAMP,
            is_locked BOOLEAN DEFAULT FALSE,
            failed_attempts INT DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS privileges (
            account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
            privilege VARCHAR(64) NOT NULL,
            PRIMARY KEY (account_id, privilege)
        );

        CREATE INDEX IF NOT EXISTS idx_accounts_name ON accounts(LOWER(name));
    )");
}

void AuthDatabasePostgreSQL::initStatements() {
    prepareStatement(STMT_CREATE_ACCOUNT,
        "INSERT INTO accounts (name, password, salt) "
        "VALUES ($1, $2, $3) RETURNING id");

    prepareStatement(STMT_AUTHENTICATE,
        "SELECT password, salt, is_locked FROM accounts "
        "WHERE LOWER(name) = LOWER($1)");

    prepareStatement(STMT_ADD_PRIVILEGE,
        "INSERT INTO privileges (account_id, privilege) "
        "VALUES ($1, $2) ON CONFLICT DO NOTHING");

    prepareStatement(STMT_REMOVE_PRIVILEGE,
        "DELETE FROM privileges WHERE account_id = $1 AND privilege = $2");

    prepareStatement(STMT_UPDATE_LOGIN_TIME,
        "UPDATE accounts SET last_login = NOW(), failed_attempts = 0 "
        "WHERE id = $1");

    prepareStatement(STMT_INCREMENT_FAILED_ATTEMPTS,
        "UPDATE accounts SET failed_attempts = failed_attempts + 1 "
        "WHERE LOWER(name) = LOWER($1)");
}

void AuthDatabasePostgreSQL::checkSchemaVersion() {
    if (!columnExists("accounts", "failed_attempts")) {
        executeQuery(
            "ALTER TABLE accounts ADD COLUMN failed_attempts INT DEFAULT 0");
    }
}

// Основные методы аутентификации

bool AuthDatabasePostgreSQL::createAuth(const std::string& name,
                                      const std::string& password) {
    validateUsername(name);

    const auto [salt, hash] = generatePasswordHash(password);

    try {
        beginTransaction();

        PGresult* res = executePrepared(STMT_CREATE_ACCOUNT, {
            name.c_str(),
            hash.c_str(),
            salt.c_str()
        }, {}, {0, 0, 0});

        if (PQntuples(res) == 0) {
            throw DatabaseException("Account creation failed");
        }

        const int account_id = std::stoi(PQgetvalue(res, 0, 0));
        commitTransaction();
        return true;

    } catch (const std::exception& e) {
        rollbackTransaction();
        errorstream << "Auth creation error: " << e.what();
        return false;
    }
}

AuthResult AuthDatabasePostgreSQL::authenticate(const std::string& name,
                                              const std::string& password) {
    AuthResult result;

    try {
        PGresult* res = executePrepared(STMT_AUTHENTICATE, {name.c_str()});

        if (PQntuples(res) == 0) {
            result.status = AUTH_NOT_FOUND;
            return result;
        }

        const std::string stored_hash = PQgetvalue(res, 0, 0);
        const std::string stored_salt = PQgetvalue(res, 0, 1);
        const bool is_locked = pg_to_bool(res, 0, 2);

        if (is_locked) {
            result.status = AUTH_LOCKED;
            return result;
        }

        const std::string computed_hash = computeSHA256(password + stored_salt);

        if (computed_hash == stored_hash) {
            result.account_id = std::stoi(PQgetvalue(res, 0, 0));
            executePrepared(STMT_UPDATE_LOGIN_TIME, {result.account_id});
            result.status = AUTH_OK;
        } else {
            executePrepared(STMT_INCREMENT_FAILED_ATTEMPTS, {name.c_str()});
            result.status = AUTH_WRONG_PASSWORD;
        }

        return result;

    } catch (const DatabaseException& e) {
        errorstream << "Authentication error: " << e.what();
        result.status = AUTH_ERROR;
        return result;
    }
}

// Управление привилегиями

void AuthDatabasePostgreSQL::addPrivilege(int account_id,
                                        const std::string& privilege) {
    validatePrivilegeName(privilege);

    executePrepared(STMT_ADD_PRIVILEGE, {
        std::to_string(account_id).c_str(),
        privilege.c_str()
    });
}

void AuthDatabasePostgreSQL::removePrivilege(int account_id,
                                           const std::string& privilege) {
    executePrepared(STMT_REMOVE_PRIVILEGE, {
        std::to_string(account_id).c_str(),
        privilege.c_str()
    });
}

// Вспомогательные методы

std::pair<std::string, std::string>
AuthDatabasePostgreSQL::generatePasswordHash(const std::string& password) {
    const std::string salt = generateRandomSalt();
    const std::string hash = computeSHA256(password + salt);
    return {salt, hash};
}

std::string AuthDatabasePostgreSQL::generateRandomSalt() const {
    std::array<unsigned char, 16> random_bytes;
    RAND_bytes(random_bytes.data(), random_bytes.size());
    return hexEncode(random_bytes.data(), random_bytes.size());
}

void AuthDatabasePostgreSQL::validateUsername(const std::string& name) const {
    if (name.empty() || name.size() > 32) {
        throw std::invalid_argument("Invalid username length");
    }

    if (name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
        != std::string::npos) {
        throw std::invalid_argument("Invalid characters in username");
    }
}

void AuthDatabasePostgreSQL::validatePrivilegeName(const std::string& privilege) const {
    if (privilege.empty() || privilege.size() > 64) {
        throw std::invalid_argument("Invalid privilege name");
    }

    if (privilege.find_first_of(" \t\n") != std::string::npos) {
        throw std::invalid_argument("Privilege name contains whitespace");
    }
}
