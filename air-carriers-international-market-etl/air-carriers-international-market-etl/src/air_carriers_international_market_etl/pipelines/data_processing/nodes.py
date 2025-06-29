import pandas as pd


def create_class_dimension(
    market_data_2024: pd.DataFrame,
    service_class: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a flight company dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Flight company dimension table.

    """
    class_dimension = pd.DataFrame()

    class_dimension = market_data_2024[["CLASS"]].drop_duplicates()
    class_dimension = class_dimension.merge(
        service_class,
        left_on="CLASS",
        right_on="Code",
        how="left",
    ).drop(columns=["CLASS"])

    return class_dimension


def create_airport_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create an airport dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Airport dimension table.

    """
    airport_dimension = pd.DataFrame()

    origin_cols = {
        "ORIGIN_AIRPORT_ID": "AIRPORT_ID",
        "ORIGIN_AIRPORT_SEQ_ID": "AIRPORT_SEQ_ID",
        "ORIGIN_CITY_MARKET_ID": "CITY_MARKET_ID",
        "ORIGIN_CITY_NAME": "CITY_NAME",
        "ORIGIN_COUNTRY": "COUNTRY",
        "ORIGIN_COUNTRY_NAME": "COUNTRY_NAME",
        "ORIGIN_WAC": "WAC",
    }
    dest_cols = {
        "DEST_AIRPORT_ID": "AIRPORT_ID",
        "DEST_AIRPORT_SEQ_ID": "AIRPORT_SEQ_ID",
        "DEST_CITY_MARKET_ID": "CITY_MARKET_ID",
        "DEST_CITY_NAME": "CITY_NAME",
        "DEST_COUNTRY": "COUNTRY",
        "DEST_COUNTRY_NAME": "COUNTRY_NAME",
        "DEST_WAC": "WAC",
    }

    origin_df = market_data_2024[list(origin_cols.keys())].rename(columns=origin_cols)
    dest_df = market_data_2024[list(dest_cols.keys())].rename(columns=dest_cols)

    airport_dimension = (
        pd.concat([origin_df, dest_df], ignore_index=True)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return airport_dimension


def create_distance_dimension(
    market_data_2024: pd.DataFrame,
    distance_groups_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Cria a dimensão de distância incluindo a descrição da faixa.

    Args:
        market_data_2024: Dados do mercado com grupos de distância.
        distance_groups_metadata: DataFrame com códigos e descrições dos grupos de distância.

    Returns:
        DataFrame com id_distancia, DISTANCE_GROUP e Faixa_Descricao.
    """
    distance_dimension = market_data_2024[["DISTANCE_GROUP"]].drop_duplicates()

    distance_groups_metadata["Code"] = distance_groups_metadata["Code"].astype(int)

    distance_dimension = distance_dimension.merge(
        distance_groups_metadata,
        left_on="DISTANCE_GROUP",
        right_on="Code",
        how="left",
    ).drop(columns=["Code"])

    distance_dimension.rename(columns={"Description": "Faixa_Descricao"}, inplace=True)

    return distance_dimension

def create_time_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a time dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024..
    Returns:
        Time dimension table.

    """
    time_dimension = pd.DataFrame()

    time_dimension = market_data_2024[["YEAR", "QUARTER", "MONTH"]].drop_duplicates()

    return time_dimension


def create_operator_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a operator dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Operator dimension table.

    """
    operator_dimension = pd.DataFrame()

    # Removing 9k from the CARRIER column
    m1 = market_data_2024["CARRIER"].str.contains("9K")

    operator_dimension = market_data_2024.loc[
        ~m1, ["CARRIER", "CARRIER_NAME"]
    ].drop_duplicates()
    operator_dimension = (
        pd.merge(
            operator_dimension,
            market_data_2024[["CARRIER", "CARRIER_GROUP", "CARRIER_GROUP_NEW"]],
            on="CARRIER",
            how="left",
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return operator_dimension


def create_company_dimension(
    market_data_2024: pd.DataFrame,
) -> pd.DataFrame:
    """Combines all data to create a company dimension table.

    Args:
        market_data_2024: Preprocessed data for market data 2024.
    Returns:
        Company dimension table.

    """
    company_dimension = pd.DataFrame()

    # Removing 9k from the CARRIER column
    m1 = market_data_2024["CARRIER"].str.contains("9K")

    company_dimension = market_data_2024.loc[
        ~m1,
        [
            "AIRLINE_ID",
            "UNIQUE_CARRIER",
            "UNIQUE_CARRIER_NAME",
            "UNIQUE_CARRIER_ENTITY",
            "CARRIER",
            "CARRIER_NAME",
            "REGION",
        ],
    ].drop_duplicates()

    company_dimension.columns

    return company_dimension

def create_fact_table(
    market_data_2024: pd.DataFrame,
    class_dimension: pd.DataFrame,
    company_dimension: pd.DataFrame,
    airport_dimension: pd.DataFrame,
    distance_dimension: pd.DataFrame,
    time_dimension: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cria tabela fato com colunas desnormalizadas para que os IDs sejam gerados
    automaticamente no ClickHouse via MATERIALIZED cityHash64(...).
    
    Returns:
        DataFrame com colunas de negócio, sem IDs substitutos.
    """
    df = market_data_2024.copy()

    if "Faixa_Descricao" in distance_dimension.columns:
        df = df.merge(distance_dimension, on="DISTANCE_GROUP", how="left")
    
    fact_table = df[
        [
            "YEAR",
            "MONTH",
            "QUARTER",
            "CLASS",
            "CARRIER",
            "UNIQUE_CARRIER",
            "ORIGIN_AIRPORT_ID",
            "ORIGIN_CITY_NAME",
            "DEST_AIRPORT_ID",
            "DEST_CITY_NAME",
            "DISTANCE_GROUP",
            "Faixa_Descricao", 
            "PASSENGERS",
            "FREIGHT",
            "MAIL",
            "DISTANCE",
        ]
    ].copy()


    fact_table["PASSENGERS"] = fact_table["PASSENGERS"].fillna(0).astype("Int64")
    fact_table["FREIGHT"] = fact_table["FREIGHT"].fillna(0).astype("Int64")
    fact_table["MAIL"] = fact_table["MAIL"].fillna(0).astype("Int64")
    fact_table["DISTANCE"] = fact_table["DISTANCE"].fillna(0).astype("Int64")

    return fact_table

