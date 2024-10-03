import clickhouse_connect


client = clickhouse_connect.get_client(host='localhost', port=8123)

names = ["trading_data_1d", "trading_data_1h", "trading_data_4h", "trading_data_15m"]
for name in names:
    csv_file_path = f"../temp/{name}.csv"

    with open(csv_file_path, 'r') as f:
        next(f)
        client.command(f"INSERT INTO {name} FORMAT CSV", data=f.read())
