# Практическая №1 — Lakehouse на TPC-H, вариант Q18 «Large Volume Customer»

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО | Власов Григорий Константинович |
| Вариант (запрос) | Q18 «Large Volume Customer» |
| Масштаб | sf1 |
| Ресурсы | Docker Desktop, 16 ГБ ОЗУ |
| Почему выбран масштаб | `tpch` в Trino отдаёт фиксированный набор схем (tiny, sf1, sf100). `tiny` слишком мал: 1 500 строк в customer — разница между кодеками тонет в погрешности. sf1 — стандартный масштаб TPC-H: 6 001 215 строк в lineitem, 1 500 000 в orders, 150 000 в customer. Этого достаточно, чтобы увидеть разницу кодеков и работу джойнов, и всё считается за секунды на ноутбуке. sf100 не поместился бы ни по диску, ни по времени. |

## 2. Поднятые сервисы

| Сервис | Роль в стеке | Почему именно он |
|---|---|---|
| SeaweedFS 4.48 (`chrislusf/seaweedfs`) | S3-совместимое объектное хранилище, куда физически падают parquet-файлы | Open source (Apache 2.0), S3 API, один контейнер. Вместо MinIO: у MinIO Community в октябре 2025 прекратили публикацию Docker-образов, репозиторий архивирован. Ceph для лабораторной слишком тяжёл. |
| PostgreSQL 16 | БД метаданных Lakekeeper (warehouse, namespace, таблицы, снапшоты) | Штатный backend Lakekeeper, надёжный, транзакционный, бесплатный. |
| Lakekeeper v0.9.5 | Iceberg REST Catalog — каталог метаданных таблиц | Open source реализация открытой спецификации Iceberg REST: любой движок (Trino, Spark, Doris) работает с ним одинаково, нет привязки к вендору, в отличие от Hive Metastore или AWS Glue. |
| Trino 470 | SQL-движок: ходит в Lakekeeper за метаданными, читает и пишет parquet в SeaweedFS | Нативный Iceberg-коннектор, встроенный `tpch`-коннектор (данные генерируются на лету), MPP-джойны для Q18. |
| DBeaver 26.2 | SQL-клиент | JDBC-драйвер Trino, видно время выполнения и результаты. |
| Apache Superset | BI-инструмент (п. 6.2) | Официальная поддержка Trino через SQLAlchemy, SQL Lab для запросов к Bronze, дашборды поверх Gold. |

**Скрипты развёртывания.** `docker-compose.yml` (проект `lakehouse-1`: seaweedfs, s3-init, postgres, lakekeeper-migrate, lakekeeper, trino, superset в профиле `bi`), `trino/catalog/*.properties` (каталоги `iceberg` и `tpch`), `create_warehouse.sh` (создание warehouse в Lakekeeper), `scripts/run_sql.sh` (запуск SQL через `docker exec trino trino --execute`). Контейнер `s3-init` создаёт бакет `lakehouse` и завершается.

**Запуск:**

```bash
docker compose up -d --build seaweedfs s3-init postgres lakekeeper-migrate lakekeeper trino
./create_warehouse.sh
docker compose --profile bi up -d --build superset
```

## 3. Разбор запроса Q18

Запрос находит заказы, в которых суммарное количество товара больше 300, и выводит топ-100 по сумме заказа вместе с клиентом.

| Таблица TPC-H | Нужные колонки | Что запрос с ними делает |
|---|---|---|
| customer | `custkey`, `name` | джойн с orders по `custkey`; вывод имени |
| orders | `orderkey`, `custkey`, `orderdate`, `totalprice` | джойн; фильтр по IN-подзапросу; группировка; сортировка по `totalprice DESC, orderdate`; вывод |
| lineitem | `orderkey`, `quantity` | подзапрос `GROUP BY orderkey HAVING sum(quantity) > 300`; джойн с orders; агрегат `sum(quantity)` |

Остальные колонки (адреса, комментарии, цены, скидки, даты поставки) запросу не нужны — поэтому Silver хранит только 6 колонок.

## 4. Медальон

Все слои — Iceberg-таблицы в каталоге `iceberg` (Lakekeeper), схемы `bronze`, `silver`, `gold`. Размер считался запросом `SELECT sum(file_size_in_bytes)/1024/1024 FROM iceberg.<схема>."<таблица>$files"` (целочисленное деление, МБ округлены вниз).

### Bronze (сырой)

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE TABLE iceberg.bronze.customer WITH (format='PARQUET') AS SELECT * FROM tpch.sf1.customer;
CREATE TABLE iceberg.bronze.orders   WITH (format='PARQUET') AS SELECT * FROM tpch.sf1.orders;
CREATE TABLE iceberg.bronze.lineitem WITH (format='PARQUET') AS SELECT * FROM tpch.sf1.lineitem;
```

| Таблица | Колонки | Строк | Вес, МБ | Формат | Сжатие | Партиц. | Время записи | Обоснование |
|---|---|---|---|---|---|---|---|---|
| customer | все (8) | 150 000 | 7 | PARQUET | ZSTD (дефолт Trino) | нет | 0,97 с | маленькая таблица: партиции дали бы лишь мелкие файлы |
| orders | все (9) | 1 500 000 | 33 | PARQUET | ZSTD | нет | 2,77 с | Q18 читает всю таблицу (джойн), фильтра по дате нет — пропускать партиции нечего |
| lineitem | все (16) | 6 001 215 | 142 | PARQUET | ZSTD | нет | 9,28 с | колоночный формат: запросу нужны 2 колонки из 16, остальные не читаются; ZSTD сжимает данные в ~2,4 раза относительно несжатого (337 МБ) |

Bronze хранит данные 1:1 из источника: ничего не фильтруется и не переименовывается, чтобы Silver всегда можно было пересобрать. Что ZSTD в Trino используется по умолчанию, подтверждается совпадением размеров: `bronze.lineitem` и явно созданная `bronze_zstd.lineitem` весят по 142 МБ.

### Silver (очищенный и обогащённый)

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.silver;

CREATE TABLE iceberg.silver.big_orders WITH (format='PARQUET') AS
SELECT orderkey, sum(quantity) AS total_quantity
FROM iceberg.bronze.lineitem
GROUP BY orderkey HAVING sum(quantity) > 300;

CREATE TABLE iceberg.silver.big_order_customers WITH (format='PARQUET') AS
SELECT trim(c.name) AS customer_name, c.custkey, o.orderkey, o.orderdate,
       CAST(o.totalprice AS DECIMAL(12,2)) AS totalprice, b.total_quantity
FROM iceberg.bronze.customer c
JOIN iceberg.bronze.orders  o ON c.custkey = o.custkey
JOIN iceberg.silver.big_orders b ON o.orderkey = b.orderkey
WHERE o.totalprice IS NOT NULL AND c.name IS NOT NULL;
```

| Таблица | Колонки | Строк | Вес | Формат | Сжатие | Партиц. | Время записи | Обоснование |
|---|---|---|---|---|---|---|---|---|
| big_orders | orderkey, total_quantity | 57 | 824 байта | PARQUET | ZSTD | нет | 0,85 с | подзапрос Q18 отсёк почти всё: из 1,5 млн заказов крупных (>300 шт.) только 57 |
| big_order_customers | customer_name, custkey, orderkey, orderdate, totalprice, total_quantity | 57 | < 0,1 МБ (единицы КБ) | PARQUET | ZSTD | нет | 0,74 с | только нужные колонки; `trim` и `CAST` нормализуют имя и сумму; NULL отфильтрованы; на 57 строках партиции бессмысленны |

После Silver в работе остаётся 57 строк вместо 7,65 млн строк в трёх Bronze-таблицах: тяжёлая агрегация lineitem и два джойна выполнены один раз.

### Gold (витрина)

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.gold;
CREATE TABLE iceberg.gold.top100_large_volume_customers WITH (format='PARQUET') AS
SELECT customer_name AS name, custkey, orderkey, orderdate, totalprice, total_quantity
FROM iceberg.silver.big_order_customers
ORDER BY totalprice DESC, orderdate LIMIT 100;
```

| Таблица | Колонки | Строк | Вес | Формат | Сжатие | Партиц. | Время записи | Обоснование |
|---|---|---|---|---|---|---|---|---|
| top100_large_volume_customers | name, custkey, orderkey, orderdate, totalprice, total_quantity | 57 | < 0,1 МБ | PARQUET | ZSTD | нет | 0,29 с | `LIMIT 100`, но на sf1 заказов с количеством >300 всего 57, поэтому в витрине 57 строк; BI читает её целиком |

**Проверка корректности.** Результат Gold совпал с исходным запросом Q18, выполненным по Bronze (`bronze_none`): те же 57 строк и те же значения. Топ-5 по `totalprice`: 544 089,09; 530 604,44; 522 720,61; 515 531,82; 508 047,99.

## 5. Тесты производительности

Для каждого кодека создана отдельная схема (`bronze_none`, `bronze_snappy`, `bronze_gzip`, `bronze_zstd`), в каждой — таблицы customer, orders, lineitem с параметром `compression_codec` в `WITH (...)`. Во всех схемах проверено `count(*) = 6 001 215` в lineitem. Q18 запускался по 3 раза на каждом кодеке; в сводной таблице — среднее.

### 5.1. Сводная таблица

| Сжатие на Bronze | Время записи lineitem, с | Q18, среднее за 3, с | Размер lineitem, МБ | Вывод |
|---|---|---|---|---|
| NONE | 8,10 | 0,894 | 337 | эталон: самые большие файлы и самый медленный Q18 |
| SNAPPY | 8,73 | 0,751 | 191 | на 43% меньше NONE; Q18 быстрее на ~16% |
| GZIP | 9,29 | 0,645 | 136 | наименьший размер и лучшее среднее время Q18 |
| ZSTD | 7,59 | 0,726 | 142 | размер почти как у GZIP, самая быстрая запись среди сжатых |
| ORC (кодек по умолчанию) | 8,20 | 0,810 | 153 | на 8% больше, чем Parquet+ZSTD; по скорости ближе к NONE |

### 5.2. Время записи по таблицам (CTAS из `tpch.sf1`), с

| Кодек | customer | orders | lineitem |
|---|---|---|---|
| NONE | 0,80 | 2,40 | 8,10 |
| SNAPPY | 1,85 | 2,60 | 8,73 |
| GZIP | 1,21 | 3,66 | 9,29 |
| ZSTD | 0,71 | 1,86 | 7,59 |

Замеры — одиночные запуски CTAS. Для SNAPPY приведены времена повторного пересоздания таблиц; при первом запуске `snappy.customer` занял 0,74 с, при повторе — 1,85 с, что показывает разброс между запусками.

### 5.3. Время Q18 по прогонам

| Кодек | run 1, мс | run 2, мс | run 3, мс | Среднее, мс |
|---|---|---|---|---|
| NONE | 846 | 866 | 969 | 894 |
| SNAPPY | 877 | 604 | 771 | 751 |
| GZIP | 568 | 623 | 745 | 645 |
| ZSTD | 613 | 751 | 813 | 726 |
| ORC | 780 | 810 | 840 | 810 |

### 5.4. Размер lineitem относительно несжатого

| Формат / сжатие | МБ | Экономия |
|---|---|---|
| PARQUET + NONE | 337 | — |
| PARQUET + SNAPPY | 191 | −43% |
| PARQUET + ZSTD | 142 | −58% |
| PARQUET + GZIP | 136 | −60% |
| ORC | 153 | −55% |

### 5.5. Слои медальона

| Слой | Q18, среднее за 3, мс | Ускорение относительно Bronze NONE |
|---|---|---|
| Bronze NONE | 894 | 1× |
| Silver `big_order_customers` | 44 | ~20× |
| Gold `top100_large_volume_customers` | 49 | ~18× |

### 5.6. Выводы по бенчмарку

- Сжатие уменьшает lineitem в 1,8–2,5 раза: SNAPPY 191 МБ, ZSTD 142 МБ, GZIP 136 МБ против 337 МБ без сжатия.
- Все сжатые варианты читаются быстрее несжатого на 16–28%: меньше данных читать с диска.
- Разброс внутри одного кодека (до 45%) сопоставим с разницей между кодеками, поэтому однозначного победителя по скорости нет: на sf1 и локальном SSD узкое место смещается с ввода-вывода на CPU-джойны.
- GZIP оказался чуть лучше ZSTD и по размеру (136 против 142 МБ), и по времени Q18 (0,645 против 0,726 с); ожидаемого преимущества ZSTD не наблюдалось. ZSTD при этом быстрее всех записал lineitem (7,59 с).
- ORC в конфигурации по умолчанию занял на 8% больше места, чем Parquet+ZSTD (153 против 142 МБ).
- Silver и Gold в ~20 раз быстрее Bronze, потому что запрос читает 57 готовых строк, а не джойнит 6 млн.

## 6. Часть 2

### 6.1. Понять и объяснить

| Вопрос | Ответ |
|---|---|
| Зачем нужны большие данные и lakehouse | Объёмы и разнородность данных растут быстрее, чем можно масштабировать обычную БД: вертикальный рост упирается в железо, шардирование сложно и дорого. Data lake решает проблему стоимости (дешёвое объектное хранилище), но не даёт транзакций, схемы и нормального SQL. Lakehouse объединяет оба подхода: данные лежат в S3 в открытом формате (Parquet), а слой Iceberg добавляет ACID-транзакции, schema evolution, снапшоты и time travel. Любой движок (Trino, Spark) читает одни и те же таблицы, поэтому нет привязки к вендору. |

**Schema evolution на Silver:**

```sql
ALTER TABLE iceberg.silver.big_order_customers ADD COLUMN order_year INTEGER;
UPDATE iceberg.silver.big_order_customers SET order_year = year(orderdate);
ALTER TABLE iceberg.silver.big_order_customers RENAME COLUMN total_quantity TO sum_quantity;
SELECT * FROM iceberg.silver.big_order_customers LIMIT 5;
```

Результат: колонка `order_year` добавлена и заполнена годом заказа, `total_quantity` переименована в `sum_quantity`, данные сохранены. Добавление и переименование колонки — изменения только метаданных Iceberg (новая версия схемы), parquet-файлы не переписываются; колонки отслеживаются по id, поэтому переименование безопасно. Строки, существовавшие до добавления колонки, получают в ней NULL, пока их не обновят; `UPDATE` создал новый снапшот типа `overwrite`.

**Time travel и восстановление:**

| Шаг | Команда | Результат |
|---|---|---|
| 1. Состояние до | `SELECT count(*) FROM iceberg.silver.big_order_customers` | 57 |
| 2. Авария | `DELETE FROM iceberg.silver.big_order_customers WHERE totalprice > 400000` | удалено 54 строки |
| 3. Состояние после | `SELECT count(*) FROM iceberg.silver.big_order_customers` | 3 |
| 4. Снапшоты | `SELECT snapshot_id, committed_at, operation FROM iceberg.silver."big_order_customers$snapshots" ORDER BY committed_at` | 4 записи (ниже) |
| 5. Чтение старой версии | `SELECT count(*) FROM iceberg.silver.big_order_customers FOR VERSION AS OF 3936641010887968933` | 57 |
| 6. Откат | `CALL iceberg.system.rollback_to_snapshot('silver', 'big_order_customers', 3936641010887968933)` | `CALL` |
| 7. Проверка | `SELECT count(*) FROM iceberg.silver.big_order_customers` | 57 |

| snapshot_id | committed_at (UTC) | operation |
|---|---|---|
| 3936641010887968933 | 2026-10-15 17:59:27 | append |
| 957507129535351057 | 2026-10-15 18:51:52 | overwrite |
| 644470630162311046 | 2026-10-15 18:56:44 | delete |
| 3982312895685816509 | 2026-10-16 10:02:55 | delete |

Для отката использован самый ранний снапшот: таблица вернулась к состоянию до UPDATE и до удалений, `count(*) = 57`. DELETE не стирает parquet-файлы, а создаёт новый снапшот, поэтому откат — это переключение таблицы на прежний снапшот и выполняется мгновенно.

### 6.2. Подключить инструмент к lakehouse: Apache Superset

| Инструмент | Почему выбран |
|---|---|
| Apache Superset | Open source BI с официальной поддержкой Trino через SQLAlchemy; подключается к тому же Trino, что и DBeaver, и видит все слои медальона. SQL Lab позволяет выполнять запросы к Bronze, а дашборды — строить визуализации поверх Gold. |

**Развёртывание.** `docker compose --profile bi up -d --build superset`. Кастомный Dockerfile: базовый образ `apache/superset:latest` и установка `trino[sqlalchemy]` в виртуальное окружение `/app/.venv` через `uv pip`. Устаревший пакет `sqlalchemy-trino` не подошёл: он несовместим с SQLAlchemy 2.x.

**Действия в UI:**

1. Settings → Database Connections → + Database → Trino.
2. SQLAlchemy URI: `trino://admin@trino:8080/iceberg` → Test Connection (успешно) → Connect. База видна в списке Databases (Backend = `trino`, Expose in SQL Lab включён).
3. SQL Lab: `SELECT name, totalprice FROM gold.top100_large_volume_customers ORDER BY totalprice DESC` — вернул 57 строк.
4. Построен bar chart (`name` × `totalprice`) и сохранён на дашборд «Lakehouse Q18».

Скриншоты: `screenshots/superset_sqllab.png`, `screenshots/superset_dashboard.png`.

## 7. Выводы

Медальон разделяет роли слоёв и кратно ускоряет аналитику: Q18 по Bronze без сжатия занимает в среднем 894 мс, по Silver — 44 мс, по Gold — 49 мс, то есть примерно в 20 раз быстрее. Тяжёлая агрегация 6-миллионного lineitem и джойны выполняются один раз при построении витрины, а Silver оставляет только нужное запросу: 57 заказов и 6 колонок. Колоночный Parquet со сжатием уменьшил lineitem с 337 МБ до 136–191 МБ в зависимости от кодека. GZIP дал наименьший размер (136 МБ, −60%) и лучшее среднее время Q18 (645 мс), ZSTD — 142 МБ и самую быструю запись среди сжатых кодеков, но разница между кодеками по времени Q18 сопоставима с разбросом самих замеров (до 45%), поэтому однозначного победителя нет; ORC в конфигурации по умолчанию оказался крупнее Parquet+ZSTD (153 против 142 МБ). Iceberg поверх объектного хранилища дал schema evolution без переписывания файлов и time travel с откатом на снапшот (57 → 3 → 57 строк), чего нет ни в обычной БД, ни в «голом» data lake. Партиционирование не использовалось сознательно: на таких объёмах оно создало бы только мелкие файлы без выигрыша.

## Структура репозитория

```
├── отчет.md                    — этот отчёт
├── docker-compose.yml          — стек: SeaweedFS, PostgreSQL, Lakekeeper, Trino, Superset
├── create_warehouse.sh         — создание warehouse в Lakekeeper
├── trino/catalog/              — конфиги каталогов Trino (iceberg, tpch)
├── superset/Dockerfile         — образ Superset с драйвером Trino
├── scripts/run_sql.sh          — запуск SQL-файлов через Trino CLI
├── sql/                        — SQL по слоям: bronze, silver, gold, benchmark
└── screenshots/                — скриншоты (docker ps, SeaweedFS, результаты, time travel, Superset)
```
