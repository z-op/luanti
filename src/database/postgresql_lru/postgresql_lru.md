# PostgreSQL Connector для Minetest

[![Лицензия](https://img.shields.io/badge/License-LGPLv2.1+-blue.svg)](LICENSE)
[![Совместимость](https://img.shields.io/badge/PostgreSQL-12%2B-brightgreen)](https://www.postgresql.org/)
[![Minetest](https://img.shields.io/badge/Minetest-5.4%2B-orange)](https://www.minetest.net/)

Высокопроизводительный асинхронный коннектор для интеграции Minetest с PostgreSQL.

## Содержание
- [Особенности](#особенности)
- [Требования](#требования)
- [Установка](#установка)
- [Использование](#использование)
- [Конфигурация](#конфигурация)
- [Безопасность](#безопасность)
- [Производительность](#производительность)
- [Лицензия](#лицензия)

## Особенности
- 🚀 Асинхронные операции с LRU-кэшированием
- 🔒 Подготовленные SQL-запросы
- 🧩 Поддержка основных компонентов:
  - Карта мира (MapDB)
  - Данные игроков (PlayerDB)
  - Аутентификация (AuthDB)
  - Хранилище модов (ModStorage)
- 📊 Мониторинг и метрики

## Требования
- Minetest 5.4+
- PostgreSQL 12+
- Библиотеки:
  - `libpq`
  - `fmt`
  - `OpenSSL`

## Установка
1. Клонировать репозиторий:
```bash
git clone https://github.com/yourrepo/minetest-pgsql.git
cd minetest-pgsql

CREATE DATABASE minetest;
CREATE USER minetest_user WITH PASSWORD 'securepass';
GRANT ALL PRIVILEGES ON DATABASE minetest TO minetest_user;

#include "MapDatabasePostgreSQL.h"

MapDatabasePostgreSQL map_db(
    "host=localhost dbname=minetest user=minetest_user",
    1000000  // Размер кэша
);

v3s16 pos(128, 64, 512);
std::string block_data = compress(mapblock);
map_db.saveBlock(pos, block_data);

std::vector<v3s16> positions = get_chunk_positions();
std::unordered_map<v3s16, std::string> blocks;
map_db.loadBlocks(positions, blocks);

ConnectionPool::Config config {
    "host=localhost dbname=minetest",
    5,   // Минимальное количество соединений
    20,  // Максимальное количество
    std::chrono::seconds(3)  // Таймаут
};

# postgresql.conf
shared_buffers = 4GB
work_mem = 64MB
maintenance_work_mem = 2GB

ssl = on
ssl_cert_file = '/path/to/server.crt'
ssl_key_file = '/path/to/server.key'

Советы по оптимизации

    Используйте BRIN-индексы для временных меток

    Настройте размер WAL:

ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET checkpoint_timeout = '30min';
