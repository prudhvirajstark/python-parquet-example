import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta

# Generate sample IoT data
num_records = 10000
start_time = datetime(2023, 1, 1, 0, 0, 0)
end_time = start_time + timedelta(minutes=num_records)

data = {
    'timestamp': pd.date_range(start=start_time, end=end_time, freq='T')[:-1],
    'temperature': [round(20 + i % 10 + 5 * (i % 100) / 100, 2) for i in range(num_records)],
    'humidity': [round(40 + i % 20 + 8 * (i % 50) / 100, 2) for i in range(num_records)],
    'occupancy': [i % 2 for i in range(num_records)]
}


print(len(data['humidity']))
print(len(data["temperature"]))
print(len(data['occupancy']))
print(len(data['timestamp']))
df = pd.DataFrame(data)

# Add a new column for date to be used for partitioning
df['date'] = df['timestamp'].dt.date

# Convert Pandas DataFrame to PyArrow Table
table = pa.Table.from_pandas(df)

# Specify the Parquet file path
parquet_file_path = 'smart_building_data.parquet'

# Write PyArrow Table to Parquet file with partitioning
pq.write_to_dataset(table, root_path=parquet_file_path, partition_cols=['date'], compression='snappy', flavor='spark',use_legacy_dataset=True)

# Read Parquet file into PyArrow Table
read_table = pq.read_table(parquet_file_path)

# Convert PyArrow Table back to Pandas DataFrame
read_df = read_table.to_pandas()

# Display original and read DataFrames
print("Original IoT Data:")
print(df.head())
print("\nRead IoT Data from Parquet:")
print(read_df.head())
