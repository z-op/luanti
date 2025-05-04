#ifndef MAP_DATABASE_POSTGRESQL_H
#define MAP_DATABASE_POSTGRESQL_H

#include "BaseDatabasePostgreSQL.h"
#include "irr_v3d.h"
#include <string>
#include <unordered_map>
#include <vector>

class MapDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    // Конструктор с настройкой подключения и кэша
    explicit MapDatabasePostgreSQL(
        const std::string& connect_string,
        size_t cache_size = 1000000
    );

    // Основные операции с блоками
    bool saveBlock(const v3s16& pos, const std::string& data);
    bool loadBlock(const v3s16& pos, std::string& data);
    bool deleteBlock(const v3s16& pos);
    bool blockExists(const v3s16& pos);

    // Массовые операции
    void saveBlocks(const std::unordered_map<v3s16, std::string>& blocks);
    void loadBlocks(const std::vector<v3s16>& positions,
                   std::unordered_map<v3s16, std::string>& results);

    // Управление данными
    void listAllBlocks(std::vector<v3s16>& positions);
    void vacuumDatabase();

protected:
    // Инициализация БД
    void createDatabase() override;
    void initStatements() override;
    void checkSchemaVersion() override;

private:
    // Подготовленные запросы
    enum PreparedStatements {
        STMT_SAVE_BLOCK,
        STMT_LOAD_BLOCK,
        STMT_DELETE_BLOCK,
        STMT_BLOCK_EXISTS,
        STMT_LIST_BLOCKS
    };

    // Вспомогательные методы
    std::string serializePosition(const v3s16& pos) const;
    v3s16 deserializePosition(const std::string& str) const;
    void validateCoordinates(const v3s16& pos) const;

    // Кэширование
    void updateCache(const v3s16& pos, const std::string& data);
    void invalidateCache(const v3s16& pos);
};

#endif // MAP_DATABASE_POSTGRESQL_H
