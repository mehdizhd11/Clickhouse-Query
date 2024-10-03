import clickhouse_connect


client = clickhouse_connect.get_client(host='localhost', port=8123)

names = ["trading_data_1d", "trading_data_1h", "trading_data_4h", "trading_data_15m"]

for table_name in names:
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    (
        open_time DateTime,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        close_time DateTime,
        quote_asset_volume Float64,
        number_of_trades UInt64,
        taker_buy_base_asset_volume Float64,
        taker_buy_quote_asset_volume Float64,
        ignore UInt64
    ) ENGINE = MergeTree()
    ORDER BY (open_time);
    """
    client.command(create_table_query)
