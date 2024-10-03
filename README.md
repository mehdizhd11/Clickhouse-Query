
---

# Trading Data Analysis with ClickHouse

This project performs analysis on trading data using ClickHouse. It calculates various statistics such as average trades, high-low differences, and rolling averages across different time intervals: 1 day, 4 hours, 1 hour, and 15 minutes.

## Project Structure

- **CSV Files**: The project assumes you have four CSV files containing trading data. Each file represents different time intervals.
- **ClickHouse Tables**: Four tables are created locally in ClickHouse to store this data:
  - `trading_data_1d`: Data for a 1-day period.
  - `trading_data_4h`: Data for a 4-hour period.
  - `trading_data_1h`: Data for a 1-hour period.
  - `trading_data_15m`: Data for a 15-minute period.

## Setup

### Prerequisites

1. **ClickHouse**: Make sure ClickHouse is installed and running locally.
2. **Python Dependencies**:
   - Install the `clickhouse-connect` Python package by running:
     ```bash
     pip install clickhouse-connect
     ```

### Importing Data

Ensure the four CSV files are properly formatted and imported into the respective ClickHouse tables.

### Table Structure

Each table should contain at least the following columns:
- `open_time`: The timestamp of the opening trade.
- `high`: The highest price during the interval.
- `low`: The lowest price during the interval.
- `number_of_trades`: The number of trades during the interval.

## Queries and Calculations

### Average Number of Trades

This function calculates the average number of trades grouped by chunks of 100 days across all periods.

```python
avg_numbers = avg_trades_number()
```

### Average High-Low Difference

This function computes the average difference between the high and low prices for each period.

```python
avg_high_low_difference = avg_high_low_difference()
```

### Daily Average High-Low Difference

This function calculates the daily sum of high-low differences and then finds the average across days.

```python
daily_avg_high_low_difference = daily_avg_high_low_difference()
```

### Rolling Average High-Low Difference and Cumulative Trades

This function computes rolling volatility (standard deviation of the high-low difference) and cumulative trades for the past 10 days for each period.

```python
rolling_avg_high_low_difference = rolling_avg_high_low_difference()
```

## Example Output

The following are sample results from running the queries:

- **Average Number of Trades**:
  - 1 Day: 151,666,628.44
  - 4 Hours: 151,666,014.36
  - 1 Hour: 151,664,875.84
  - 15 Minutes: 151,902,099.24

- **Average High-Low Difference**:
  - 1 Day: 1304.60
  - 4 Hours: 512.30
  - 1 Hour: 249.04
  - 15 Minutes: 120.92

- **Daily Average High-Low Difference**:
  - 1 Day: 1307.27
  - 4 Hours: 3070.22
  - 1 Hour: 5963.75
  - 15 Minutes: 11577.16

- **Rolling Average High-Low Difference and Cumulative Trades**:
  - 1 Day: 
    - Volatility: 0.83
    - Cumulative Trades: 15,514,973.10
  - 4 Hours:
    - Volatility: 209.74
    - Cumulative Trades: 15,483,186.86
  - 1 Hour:
    - Volatility: 131.16
    - Cumulative Trades: 15,483,175.20
  - 15 Minutes:
    - Volatility: 72.62
    - Cumulative Trades: 15,495,746.79

## Usage

To run the project, simply execute the Python script that includes the queries:

```bash
python main.py
```

Make sure that your ClickHouse server is running on `localhost:8123` and that the required CSV data has been imported into the respective tables.

---