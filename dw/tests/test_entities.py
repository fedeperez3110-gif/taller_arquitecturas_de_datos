from dw.entities import ALL_ENTITIES


def test_parquet_and_redshift_columns_are_in_the_same_order():
    # COPY FORMAT AS PARQUET asigna por posición
    for e in ALL_ENTITIES:
        ddl_cols = [line.strip().split()[0] for line in e.redshift_ddl().splitlines()[1:-1]]
        assert ddl_cols == e.arrow_schema().names


def test_raw_tables_live_in_raw_data_schema():
    assert {e.raw_table for e in ALL_ENTITIES} == {
        "raw_data.mysql_customers", "raw_data.mysql_products", "raw_data.mysql_orders",
        "raw_data.mysql_order_items", "raw_data.mongo_reviews", "raw_data.mongo_navigation_events"}
