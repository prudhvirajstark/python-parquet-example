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

df['date'] = df['timestamp'].dt.date
table = pa.Table.from_pandas(df)

parquet_file_path = 'smart_building_data.parquet'
pq.write_to_dataset(table, root_path=parquet_file_path, partition_cols=['date'], compression='snappy', flavor='spark',use_legacy_dataset=True)
read_table = pq.read_table(parquet_file_path)

read_df = read_table.to_pandas()

print("Original IoT Data:")
print(df.head())
print("\nRead IoT Data from Parquet:")
print(read_df.head())
