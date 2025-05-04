#ifndef PLAYER_DATABASE_POSTGRESQL_H
#define PLAYER_DATABASE_POSTGRESQL_H

#include "base_database_postgresql.h"
#include "inventory.h"
#include "irr_v3d.h"
#include <unordered_map>
#include <string>
#include <chrono>

class PlayerDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    // Структура данных игрока
    struct PlayerData {
        v3f position;
        float pitch = 0.0f;
        float yaw = 0.0f;
        int hp = 20;
        int breath = 0;
        Inventory inventory;
        std::unordered_map<std::string, std::string> metadata;
    };

    explicit PlayerDatabasePostgreSQL(const std::string& connect_string);

    // Основные операции
    bool savePlayer(const std::string& name, const PlayerData& data);
    bool loadPlayer(const std::string& name, PlayerData& data);
    bool removePlayer(const std::string& name);

    // Управление метаданными
    void savePlayerMetadata(const std::string& name,
                          const std::unordered_map<std::string, std::string>& metadata);
    void loadPlayerMetadata(const std::string& name,
                          std::unordered_map<std::string, std::string>& output);

    // Администрирование
    void cleanupOldPlayers(std::chrono::years max_age);

protected:
    // Инициализация БД
    void createDatabase() override;
    void initStatements() override;
    void checkSchemaVersion() override;

private:
    // Подготовленные запросы
    enum PreparedStatements {
        STMT_SAVE_PLAYER,
        STMT_LOAD_PLAYER,
        STMT_SAVE_METADATA,
        STMT_LOAD_METADATA,
        STMT_REMOVE_PLAYER
    };

    // Вспомогательные методы
    void saveMetadataBatch(const std::string& name,
                         const std::unordered_map<std::string, std::string>& metadata);
    void validatePlayerName(const std::string& name) const;

    // Сериализация
    std::string serializeInventory(const Inventory& inv) const;
    void deserializeInventory(const std::string& data, Inventory& inv) const;
};

#endif // PLAYER_DATABASE_POSTGRESQL_H
