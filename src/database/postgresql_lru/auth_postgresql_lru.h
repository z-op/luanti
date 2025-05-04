#ifndef AUTHDATABASEPOSTGRESQL_H
#define AUTHDATABASEPOSTGRESQL_H

#include "BaseDatabasePostgreSQL.h"
#include <string>
#include <stdexcept>

class AuthDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    // Результат аутентификации
    struct AuthResult {
        enum class Status {
            Success,
            InvalidCredentials,
            AccountLocked,
            DatabaseError,
            InvalidInput
        };

        Status status;
        std::string errorMessage;
    };

    // Информация об аккаунте
    struct AccountInfo {
        std::string username;
        std::string passwordHash;
        std::string salt;
        int failedAttempts = 0;
        bool isLocked = false;
    };

    explicit AuthDatabasePostgreSQL(const std::string& connectionString);

    // Основные методы
    AuthResult createAccount(const std::string& username, const std::string& password);
    AuthResult authenticate(const std::string& username, const std::string& password);
    AuthResult updatePassword(const std::string& username, const std::string& newPassword);
    AuthResult lockAccount(const std::string& username);
    AuthResult unlockAccount(const std::string& username);

private:
    // Константы безопасности
    static constexpr int MAX_USERNAME_LENGTH = 32;
    static constexpr int MIN_PASSWORD_LENGTH = 8;
    static constexpr int MAX_FAILED_ATTEMPTS = 5;
    static constexpr int SALT_LENGTH = 32;

    // Вспомогательные методы
    bool validateCredentials(const std::string& username, const std::string& password) const;
    std::string generateSalt() const;
    std::string hashPassword(const std::string& password, const std::string& salt) const;
    bool verifyPassword(const std::string& password, const AccountInfo& accountInfo) const;
    void handleDatabaseError(AuthResult& result, const std::string& context) const;

    // SQL-запросы
    void initPreparedStatements() override;
};

#endif // AUTHDATABASEPOSTGRESQL_H
