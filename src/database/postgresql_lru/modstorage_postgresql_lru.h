#ifndef MOD_STORAGE_DATABASE_POSTGRESQL_H
#define MOD_STORAGE_DATABASE_POSTGRESQL_H

#include "base_database_postgresql.h"
#include <string>
#include <vector>
#include <chrono>

class ModStorageDatabasePostgreSQL : public BaseDatabasePostgreSQL {
public:
    /**
     * @brief Конструктор хранилища данных модов
     * @param connect_string Строка подключения к PostgreSQL
     * @param cache_size Размер LRU-кэша (по умолчанию 1,000,000 записей)
     */
    explicit ModStorageDatabasePostgreSQL(
        const std::string& connect_string,
        size_t cache_size = 1000000
    );

    /**
     * @brief Сохраняет данные мода с TTL
     * @param modname Идентификатор мода
     * @param key Ключ данных
     * @param value Значение (бинарные данные)
     * @param ttl Время жизни записи в секундах (0 = бессрочно)
     */
    void setModEntry(const std::string& modname,
                   const std::string& key,
                   const std::string& value,
                   std::chrono::seconds ttl = std::chrono::seconds(0));

    /**
     * @brief Получает данные мода
     * @param modname Идентификатор мода
     * @param key Ключ данных
     * @param value[out] Приемник данных
     * @return true если данные найдены и не просрочены
     */
    bool getModEntry(const std::string& modname,
                    const std::string& key,
                    std::string& value);

    /**
     * @brief Удаляет конкретную запись мода
     * @param modname Идентификатор мода
     * @param key Ключ данных
     */
    void removeModEntry(const std::string& modname,
                      const std::string& key);

    /**
     * @brief Удаляет все записи мода
     * @param modname Идентификатор мода
     */
    void removeAllEntries(const std::string& modname);

    /**
     * @brief Возвращает список всех ключей мода
     * @param modname Идентификатор мода
     * @return Вектор строк с ключами
     */
    std::vector<std::string> listKeys(const std::string& modname);

    /**
     * @brief Очищает просроченные записи
     * @details Удаляет все записи с истекшим TTL
     */
    void cleanupExpired();

protected:
    // Наследуемые методы базового класса
    void createDatabase() override;
    void initStatements() override;
    void checkSchemaVersion() override;

private:
    enum PreparedStatements {
        STMT_SET_ENTRY,
        STMT_GET_ENTRY,
        STMT_REMOVE_ENTRY,
        STMT_REMOVE_ALL,
        STMT_LIST_KEYS,
        STMT_CLEANUP
    };

    /// Генерация ключа для кэша
    std::string makeCacheKey(const std::string& modname,
                           const std::string& key) const;

    /// Валидация ключа
    void validateKey(const std::string& key) const;
};

#endif // MOD_STORAGE_DATABASE_POSTGRESQL_H
