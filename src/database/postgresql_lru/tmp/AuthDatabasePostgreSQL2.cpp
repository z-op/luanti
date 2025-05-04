#include "AuthDatabasePostgreSQL.h"
#include "DatabaseException.h"
#include "Util/StringUtils.h"
#include <openssl/sha.h>
#include <random>
#include <algorithm>

// Конструктор с инициализацией подключения
AuthDatabasePostgreSQL::AuthDatabasePostgreSQL(const std::string& connectionStr)
    : BaseDatabasePostgreSQL(connectionStr, "auth")
{
    initPreparedStatements();
}

// Инициализация подготовленных SQL-запросов
void AuthDatabasePostgreSQL::initPreparedStatements() {
    prepareStatement(
        "CREATE_ACCOUNT",
        "INSERT INTO auth.accounts (username, password_hash, salt, created_at) "
        "VALUES ($1, $2, $3, NOW())"
    );

    prepareStatement(
        "GET_ACCOUNT_INFO",
        "SELECT password_hash, salt, failed_attempts, is_locked FROM auth.accounts "
        "WHERE username = $1"
    );

    prepareStatement(
        "UPDATE_LOGIN_ATTEMPT",
        "UPDATE auth.accounts SET "
        "last_login_attempt = NOW(), "
        "failed_attempts = failed_attempts + 1 "
        "WHERE username = $1"
    );

    prepareStatement(
        "RESET_FAILED_ATTEMPTS",
        "UPDATE auth.accounts SET "
        "failed_attempts = 0, last_successful_login = NOW() "
        "WHERE username = $1"
    );
}

// Создание аккаунта с валидацией данных
AuthResult AuthDatabasePostgreSQL::createAccount(const AccountInfo& info) {
    AuthResult result;

    if (!validateCredentials(info.username, info.password)) {
        result.status = AuthStatus::INVALID_INPUT;
        return result;
    }

    try {
        const auto [salt, hash] = generatePasswordHash(info.password);

        executePrepared(
            "CREATE_ACCOUNT",
            { info.username, hash, salt }
        );

        result.status = AuthStatus::SUCCESS;
    }
    catch (const DatabaseException& e) {
        handleDatabaseError(e, result);
    }

    return result;
}

// Аутентификация пользователя с защитой от brute-force
AuthResult AuthDatabasePostgreSQL::authenticate(const std::string& username,
                                              const std::string& password) {
    AuthResult result;

    try {
        auto res = executePrepared("GET_ACCOUNT_INFO", { username });

        if (res.empty()) {
            result.status = AuthStatus::USER_NOT_FOUND;
            return result;
        }

        const auto& row = res[0];
        AccountSecurityInfo securityInfo = {
            .hash = row["password_hash"],
            .salt = row["salt"],
            .failedAttempts = std::stoi(row["failed_attempts"]),
            .isLocked = row["is_locked"] == "t"
        };

        if (securityInfo.isLocked) {
            result.status = AuthStatus::ACCOUNT_LOCKED;
            return result;
        }

        if (verifyPassword(password, securityInfo)) {
            executePrepared("RESET_FAILED_ATTEMPTS", { username });
            result.status = AuthStatus::SUCCESS;
        } else {
            executePrepared("UPDATE_LOGIN_ATTEMPT", { username });
            result.status = (securityInfo.failedAttempts + 1 >= MAX_FAILED_ATTEMPTS)
                          ? AuthStatus::ACCOUNT_LOCKED
                          : AuthStatus::WRONG_PASSWORD;
        }
    }
    catch (const DatabaseException& e) {
        handleDatabaseError(e, result);
    }

    return result;
}

// Генерация криптографически безопасной соли
std::string AuthDatabasePostgreSQL::generateSalt() const {
    std::random_device rd;
    std::uniform_int_distribution<int> dist(0, 255);

    std::string salt(SALT_LENGTH, 0);
    std::generate_n(salt.begin(), SALT_LENGTH, [&] { return dist(rd); });

    return salt;
}

// Хэширование пароля с использованием PBKDF2
std::string AuthDatabasePostgreSQL::hashPassword(
    const std::string& password,
    const std::string& salt
) const {
    constexpr int iterations = 10000;
    constexpr int keyLength = 32; // 256 бит

    std::vector<unsigned char> output(keyLength);

    PKCS5_PBKDF2_HMAC(
        password.data(), password.size(),
        reinterpret_cast<const unsigned char*>(salt.data()), salt.size(),
        iterations,
        EVP_sha256(),
        keyLength,
        output.data()
    );

    return std::string(output.begin(), output.end());
}

// Верификация пароля с защитой от timing-атак
bool AuthDatabasePostgreSQL::verifyPassword(
    const std::string& password,
    const AccountSecurityInfo& securityInfo
) const {
    std::string computedHash = hashPassword(password, securityInfo.salt);
    return constantTimeCompare(computedHash, securityInfo.hash);
}

// Сравнение строк за постоянное время
bool AuthDatabasePostgreSQL::constantTimeCompare(
    const std::string& a,
    const std::string& b
) const {
    return CRYPTO_memcmp(a.data(), b.data(), std::min(a.size(), b.size())) == 0
           && a.size() == b.size();
}

// Обработка ошибок базы данных
void AuthDatabasePostgreSQL::handleDatabaseError(
    const DatabaseException& e,
    AuthResult& result
) const {
    result.status = AuthStatus::DATABASE_ERROR;
    result.errorMessage = e.what();

    // Логирование ошибки в системный журнал
    syslog(LOG_ERR, "Auth database error: %s", e.what());
}

// Валидация входных данных
bool AuthDatabasePostgreSQL::validateCredentials(
    const std::string& username,
    const std::string& password
) const {
    return !username.empty()
        && username.length() <= MAX_USERNAME_LENGTH
        && !password.empty()
        && password.length() >= MIN_PASSWORD_LENGTH
        && std::all_of(username.begin(), username.end(), [](char c) {
            return isalnum(c) || c == '_' || c == '-';
        });
}
