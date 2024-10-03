import clickhouse_connect


client = clickhouse_connect.get_client(host='localhost', port=8123)


def avg_trades_number():
    queries = {
        '1 Day': "SELECT AVG(ten_day_sum) FROM (SELECT SUM(number_of_trades) AS ten_day_sum "
                 "FROM trading_data_1d GROUP BY intDiv(toRelativeDayNum(open_time), 100));",
        '4 Hours': "SELECT AVG(ten_day_sum) FROM (SELECT SUM(number_of_trades) AS ten_day_sum "
                   "FROM trading_data_4h GROUP BY intDiv(toRelativeDayNum(open_time), 100));",
        '1 Hour': "SELECT AVG(ten_day_sum) FROM (SELECT SUM(number_of_trades) AS ten_day_sum "
                  "FROM trading_data_1h GROUP BY intDiv(toRelativeDayNum(open_time), 100));",
        '15 Minutes': "SELECT AVG(ten_day_sum) FROM (SELECT SUM(number_of_trades) AS ten_day_sum "
                      "FROM trading_data_15m GROUP BY intDiv(toRelativeDayNum(open_time), 100));",
    }

    results = {}

    for period, query in queries.items():
        result = client.query(query).result_rows[0][0]
        results[period] = result

    for period, avg_number in results.items():
        print(f"Period: {period}, Average Number: {avg_number}")

    return results


# Period: 1 Day, Average Number: 151666628.44
# Period: 4 Hours, Average Number: 151666014.36
# Period: 1 Hour, Average Number: 151664875.84
# Period: 15 Minutes, Average Number: 151902099.24
avg_numbers = avg_trades_number()


def avg_high_low_difference():
    queries = {
        '1 Day': "SELECT AVG(high - low) AS avg_difference FROM trading_data_1d;",
        '4 Hours': "SELECT AVG(high - low) AS avg_difference FROM trading_data_4h;",
        '1 Hour': "SELECT AVG(high - low) AS avg_difference FROM trading_data_1h;",
        '15 Minutes': "SELECT AVG(high - low) AS avg_difference FROM trading_data_15m;",
    }

    results = {}

    for period, query in queries.items():
        result = client.query(query).result_rows[0][0]
        results[period] = result

    for period, avg_number in results.items():
        print(f"Period: {period}, Average Difference: {avg_number}")

    return results


# Period: 1 Day, Average Difference: 1304.5951863990165
# Period: 4 Hours, Average Difference: 512.2985494565588
# Period: 1 Hour, Average Difference: 249.04227597769184
# Period: 15 Minutes, Average Difference: 120.91643941692556
avg_high_low_difference = avg_high_low_difference()


def daily_avg_high_low_difference():
    queries = {
        '1 Day': "SELECT AVG(daily_diff) AS avg_difference FROM ( SELECT toDate(open_time) AS day, "
                 "SUM(high - low) AS daily_diff FROM trading_data_1d GROUP BY day);",
        '4 Hours': "SELECT AVG(daily_diff) AS avg_difference FROM ( SELECT toDate(open_time) AS day, "
                   "SUM(high - low) AS daily_diff FROM trading_data_4h GROUP BY day);",
        '1 Hour': "SELECT AVG(daily_diff) AS avg_difference FROM ( SELECT toDate(open_time) AS day, "
                  "SUM(high - low) AS daily_diff FROM trading_data_1h GROUP BY day);",
        '15 Minutes': "SELECT AVG(daily_diff) AS avg_difference FROM ( SELECT toDate(open_time) AS day, "
                      "SUM(high - low) AS daily_diff FROM trading_data_15m GROUP BY day);",
    }

    results = {}

    for period, query in queries.items():
        result = client.query(query).result_rows[0][0]
        results[period] = result

    for period, avg_number in results.items():
        print(f"Period: {period}, Daily Average Difference: {avg_number}")

    return results


# Period: 1 Day, Daily Average Difference: 1307.2729269293925
# Period: 4 Hours, Daily Average Difference: 3070.223465792707
# Period: 1 Hour, Daily Average Difference: 5963.751413355182
# Period: 15 Minutes, Daily Average Difference: 11577.155376432078
daily_avg_high_low_difference = daily_avg_high_low_difference()


def rolling_avg_high_low_difference():
    queries = {
        '1 Day': "SELECT AVG(rolling_volatility) AS avg_volatility, AVG(ten_day_cumulative_trades) "
                 "AS avg_cumulative_trades FROM (SELECT toDate(open_time) AS day, stddevPop(high - low) AS rolling_volatility"
                 ",SUM(number_of_trades) AS total_trades,"
                 "SUM(SUM(number_of_trades)) OVER (ORDER BY day ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) "
                 "AS ten_day_cumulative_trades FROM trading_data_1d  GROUP BY day   ) ;",
        '4 Hours': "SELECT AVG(rolling_volatility) AS avg_volatility, AVG(ten_day_cumulative_trades) "
                   "AS avg_cumulative_trades FROM (SELECT toDate(open_time) AS day, stddevPop(high - low) AS rolling_volatility"
                   ",SUM(number_of_trades) AS total_trades,"
                   "SUM(SUM(number_of_trades)) OVER (ORDER BY day ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) "
                   "AS ten_day_cumulative_trades FROM trading_data_4h  GROUP BY day   ) ;",
        '1 Hour': "SELECT AVG(rolling_volatility) AS avg_volatility, AVG(ten_day_cumulative_trades) "
                  "AS avg_cumulative_trades FROM (SELECT toDate(open_time) AS day, stddevPop(high - low) AS rolling_volatility"
                  ",SUM(number_of_trades) AS total_trades,"
                  "SUM(SUM(number_of_trades)) OVER (ORDER BY day ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) "
                  "AS ten_day_cumulative_trades FROM trading_data_1h  GROUP BY day   ) ;",
        '15 Minutes': "SELECT AVG(rolling_volatility) AS avg_volatility, AVG(ten_day_cumulative_trades) "
                      "AS avg_cumulative_trades FROM (SELECT toDate(open_time) AS day, stddevPop(high - low) AS rolling_volatility"
                      ",SUM(number_of_trades) AS total_trades,"
                      "SUM(SUM(number_of_trades)) OVER (ORDER BY day ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) "
                      "AS ten_day_cumulative_trades FROM trading_data_15m GROUP BY day) ;",
    }

    results = {}

    for period, query in queries.items():
        result = client.query(query).result_rows[0]
        results[period] = {
            'Average Volatility': result[0],
            'Average Cumulative Trades': result[1]
        }

    for period, stats in results.items():
        print(f"Average Volatility: {stats['Average Volatility']}, "
              f"Average Cumulative Trades: {stats['Average Cumulative Trades']}")

    return results


# Average Volatility: 0.8296469622331699, Average Cumulative Trades: 15514973.104269294
# Average Volatility: 209.7441708577097, Average Cumulative Trades: 15483186.861941827
# Average Volatility: 131.15947951462493, Average Cumulative Trades: 15483175.201556738
# Average Volatility: 72.61819275615818, Average Cumulative Trades: 15495746.787643207
rolling_avg_high_low_difference = rolling_avg_high_low_difference()
