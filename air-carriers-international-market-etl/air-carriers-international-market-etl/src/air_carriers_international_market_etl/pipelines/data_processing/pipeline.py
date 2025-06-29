from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    create_airport_dimension,
    create_class_dimension,
    create_company_dimension,
    create_distance_dimension,
    create_operator_dimension,
    create_time_dimension,
    create_fact_table,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_class_dimension,
                inputs=["market_data_2024", "service_class"],
                outputs="class_dimension",
                name="create_class_dimension_node",
            ),
            node(
                func=create_airport_dimension,
                inputs="market_data_2024",
                outputs="airport_dimension",
                name="create_airport_dimension_node",
            ),
            node(
                func=create_distance_dimension,
                inputs=["market_data_2024","distance_group_500"],
                outputs="distance_dimension",
                name="create_distance_dimension_node",
            ),
            node(
                func=create_time_dimension,
                inputs="market_data_2024",
                outputs="time_dimension",
                name="create_time_dimension_node",
            ),
            node(
                func=create_operator_dimension,
                inputs="market_data_2024",
                outputs="operator_dimension",
                name="create_operator_dimension_node",
            ),
            node(
                func=create_company_dimension,
                inputs="market_data_2024",
                outputs="company_dimension",
                name="create_company_dimension_node",
            ),
            node(
                func=create_fact_table,
                inputs=[
                    "market_data_2024",
                    "class_dimension",
                    "company_dimension",
                    "airport_dimension",
                    "distance_dimension",
                    "time_dimension",
                ],
                outputs="fato_transporte_aereo",
                name="create_fact_table_node",
            )

        ]
    )
