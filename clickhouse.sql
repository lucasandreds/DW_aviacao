CREATE TABLE IF NOT EXISTS Dim_Companhia
(
    id_companhia UInt64 MATERIALIZED cityHash64(CARRIER, UNIQUE_CARRIER),

    AIRLINE_ID LowCardinality(String) CODEC(ZSTD(1)),
    UNIQUE_CARRIER LowCardinality(FixedString(3)) CODEC(LZ4),
    UNIQUE_CARRIER_NAME String CODEC(ZSTD(3)),
    UNIQUE_CARRIER_ENTITY LowCardinality(String) CODEC(ZSTD(3)),
    CARRIER LowCardinality(FixedString(3)) CODEC(LZ4),
    CARRIER_NAME String CODEC(ZSTD(3)),
    REGION LowCardinality(String) CODEC(ZSTD(1)),

    INDEX idx_carrier CARRIER TYPE bloom_filter(0.01) GRANULARITY 3
)
ENGINE = ReplacingMergeTree()--MergeTree()
ORDER BY id_companhia
PRIMARY KEY id_companhia;

CREATE TABLE IF NOT EXISTS Dim_Operadora 
(
    id_operadora UInt64 MATERIALIZED cityHash64(CARRIER, CARRIER_GROUP_NEW),

    CARRIER LowCardinality(FixedString(3)) CODEC(LZ4),
    CARRIER_NAME String CODEC(ZSTD(3)),
    CARRIER_GROUP LowCardinality(String) CODEC(ZSTD(1)),
    CARRIER_GROUP_NEW LowCardinality(String) CODEC(ZSTD(1)),

    INDEX idx_operadora CARRIER TYPE bloom_filter(0.01) GRANULARITY 3
)
ENGINE = ReplacingMergeTree()--MergeTree()
ORDER BY id_operadora
PRIMARY KEY id_operadora;

CREATE TABLE IF NOT EXISTS Dim_Aeroporto
(
    id_aeroporto UInt64 MATERIALIZED cityHash64(AIRPORT_ID, CITY_NAME),

    AIRPORT_ID LowCardinality(String) CODEC(LZ4),
    AIRPORT_SEQ_ID LowCardinality(String) CODEC(ZSTD(1)),
    CITY_MARKET_ID LowCardinality(String) CODEC(ZSTD(1)),
    CITY_NAME LowCardinality(String) CODEC(ZSTD(3)),
    COUNTRY LowCardinality(String) CODEC(ZSTD(1)),
    COUNTRY_NAME String CODEC(ZSTD(3)),
    WAC LowCardinality(String) CODEC(ZSTD(1)),

    INDEX idx_airport AIRPORT_ID TYPE bloom_filter(0.01) GRANULARITY 3
)
ENGINE = ReplacingMergeTree()--MergeTree()
ORDER BY id_aeroporto
PRIMARY KEY id_aeroporto;

CREATE TABLE IF NOT EXISTS Dim_Tempo
(
    id_tempo UInt64 MATERIALIZED cityHash64(YEAR, MONTH),

    YEAR UInt16 CODEC(Delta, ZSTD(1)),
    QUARTER UInt8 CODEC(T64, ZSTD(1)),
    MONTH UInt8 CODEC(T64, ZSTD(1)),

    INDEX idx_ano YEAR TYPE set(100) GRANULARITY 4
)

ENGINE = ReplacingMergeTree()--MergeTree()
ORDER BY id_tempo
PRIMARY KEY id_tempo;


CREATE TABLE IF NOT EXISTS Dim_Distancia
(
    id_distancia UInt64 MATERIALIZED cityHash64(DISTANCE_GROUP, Faixa_Descricao),

    DISTANCE_GROUP UInt8 CODEC(T64, ZSTD(1)),
    Faixa_Descricao LowCardinality(String) CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree()--MergeTree()
ORDER BY id_distancia
PRIMARY KEY id_distancia;

CREATE TABLE IF NOT EXISTS Dim_Class
(
    id_class UInt64 MATERIALIZED cityHash64(CLASS),

    CLASS LowCardinality(String) CODEC(LZ4),
    Descricao_CLASS String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree()--MergeTree()
ORDER BY id_class
PRIMARY KEY id_class;

CREATE TABLE raw_fato
(
    YEAR UInt16,
    MONTH UInt8,
    QUARTER UInt8,
    CLASS String,
    CARRIER String,
    UNIQUE_CARRIER String,
    ORIGIN_AIRPORT_ID String,
    ORIGIN_CITY_NAME String,
    DEST_AIRPORT_ID String,
    DEST_CITY_NAME String,
    DISTANCE_GROUP UInt8,
    Faixa_Descricao String,
    PASSENGERS UInt32,
    FREIGHT Float32,
    MAIL Float32,
    DISTANCE Float32
)
ENGINE = MergeTree()
ORDER BY (YEAR, MONTH, CARRIER);


CREATE TABLE IF NOT EXISTS Fato_TransporteAereo
(
    id_tempo UInt32,
    id_companhia UInt64,
    id_origem UInt64,
    id_destino UInt64,
    id_distancia UInt64,
    id_class UInt64,

    PASSENGERS UInt32 CODEC(Delta, ZSTD(3)),
    FREIGHT Float32 CODEC(ZSTD(2)),
    MAIL Float32 CODEC(ZSTD(2)),
    DISTANCE Float32 CODEC(ZSTD(2))
)
ENGINE = MergeTree()
PARTITION BY id_tempo
ORDER BY (id_tempo, id_companhia, id_origem, id_destino)
SETTINGS index_granularity = 8192;

INSERT INTO Dim_Operadora ( --Funcionou
    CARRIER,
    CARRIER_NAME,
    CARRIER_GROUP,
    CARRIER_GROUP_NEW
)
FROM INFILE 'teste/Operadora.csv'
FORMAT CSVWithNames;

INSERT INTO Dim_Companhia ( --Funcionou
    AIRLINE_ID,
    UNIQUE_CARRIER,
    UNIQUE_CARRIER_NAME,
    UNIQUE_CARRIER_ENTITY,
    CARRIER,
    CARRIER_NAME,
    REGION
)
FROM INFILE 'teste/Companhia.csv'
FORMAT CSVWithNames;

INSERT INTO Dim_Aeroporto (
    AIRPORT_ID,
    AIRPORT_SEQ_ID,
    CITY_MARKET_ID,
    CITY_NAME,
    COUNTRY,
    COUNTRY_NAME,
    WAC
)
FROM INFILE 'teste/Aeroporto.csv'
FORMAT CSVWithNames;

INSERT INTO Dim_Tempo ( --Funcionou
    YEAR,
    QUARTER,
    MONTH
)
FROM INFILE 'teste/Tempo.csv'
FORMAT CSVWithNames;


INSERT INTO Dim_Distancia (
    DISTANCE_GROUP,
    Faixa_Descricao
)
FROM INFILE 'teste/Distancia.csv'
FORMAT CSVWithNames;

INSERT INTO Dim_Class (
    CLASS,
    Descricao_CLASS
)
FROM INFILE 'teste/Class.csv'
FORMAT CSVWithNames;

INSERT INTO raw_fato FROM INFILE 'teste/Fato.csv' FORMAT CSVWithNames;

INSERT INTO Fato_TransporteAereo
SELECT
    Dim_Tempo.id_tempo,
    Dim_Companhia.id_companhia,
    origem.id_aeroporto AS id_origem,
    destino.id_aeroporto AS id_destino,
    Dim_Distancia.id_distancia,
    Dim_Class.id_class,
    raw.PASSENGERS,
    raw.FREIGHT,
    raw.MAIL,
    raw.DISTANCE
FROM raw_fato AS raw
LEFT JOIN Dim_Tempo
    ON raw.YEAR = Dim_Tempo.YEAR AND raw.MONTH = Dim_Tempo.MONTH
LEFT JOIN Dim_Companhia
    ON raw.CARRIER = Dim_Companhia.CARRIER AND raw.UNIQUE_CARRIER = Dim_Companhia.UNIQUE_CARRIER
LEFT JOIN Dim_Class
    ON raw.CLASS = Dim_Class.CLASS
LEFT JOIN Dim_Distancia
    ON raw.DISTANCE_GROUP = Dim_Distancia.DISTANCE_GROUP AND raw.Faixa_Descricao = Dim_Distancia.Faixa_Descricao
LEFT JOIN Dim_Aeroporto AS origem
    ON raw.ORIGIN_AIRPORT_ID = origem.AIRPORT_ID AND raw.ORIGIN_CITY_NAME = origem.CITY_NAME
LEFT JOIN Dim_Aeroporto AS destino
    ON raw.DEST_AIRPORT_ID = destino.AIRPORT_ID AND raw.DEST_CITY_NAME = destino.CITY_NAME;
