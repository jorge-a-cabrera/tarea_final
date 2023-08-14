CREATE STREAM stock_dato (symbol VARCHAR, price DOUBLE, volume INT, timestamp VARCHAR)
  WITH (kafka_topic='stock-pedido', value_format='json', partitions=1);

CREATE TABLE pregunta_1 AS
    SELECT symbol,
           avg(price/volume) AS avg_pond
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

CREATE TABLE pregunta_2 AS
    SELECT symbol,
           count(symbol) AS transac
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

CREATE TABLE pregunta_3 AS
    SELECT symbol,
           max(price) AS max_price
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

CREATE TABLE pregunta_4 AS
    SELECT symbol,
           min(price) AS min_price
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

-- Esta ultima consulta solo consolida en una sola los cuatros requerimientos
-- anteriores, asi como la sumatoria de las columnas price y volume
CREATE TABLE pregunta_all AS
    SELECT symbol,
           sum(price) AS sum_price,
           sum(volume) AS sum_volume,
           avg(price/volume) AS avg_pond,
           count(symbol) AS transac,
           max(price) AS max_price,
           min(price) AS min_price
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

SELECT * FROM pregunta_1;
SELECT * FROM pregunta_2;
SELECT * FROM pregunta_3;
SELECT * FROM pregunta_4;
SELECT * FROM pregunta_all;
