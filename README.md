# RedisDataHandler
Utility to manage data interactions with Redis database
 
# Usage
Initialize RedisDataHandler
``` 
from redis_data_handler import RedisDataHandler
```
# Initialize RedisDataHandler
```
redis_handler = RedisDataHandler(host='localhost', port=6379, db=0)
```
# Publish Data to Redis
```
import pandas as pd
```
# Sample DataFrame
```
data = {'timestamp': ['2023-01-01', '2023-01-02'], 'value': [100, 200]}
df = pd.DataFrame(data)
```
# Publish DataFrame to Redis
```
last_index = 0
redis_handler.publish_dataframe(df, 'my_dataframe_key', last_published_index=last_index, publish=True)
```
# Publish JSON Data
```
data = {"name": "John", "age": 30}
redis_handler.publish_to_redis_json(data, 'my_json_key', publish=True)
```
# Publish String Data
```
redis_handler.publish_to_redis_str("Hello, Redis!", 'my_string_key', publish=False)
```
# Retrieve Data from Redis

# Retrieve a DataFrame
```
df = redis_handler.retrieve_dataframe_from_redis('my_dataframe_key')
print(df)
```
# Retrieve JSON Data
```
data = redis_handler.retrieve_json_from_redis('my_json_key')
print(data)
```

# Retrieve String Data
```
message = redis_handler.retrieve_str_from_redis('my_string_key')
print(message)
```
# Manage Redis Keys
# Delete Keys
```
result = redis_handler.delete_keys(['key1', 'key2'])
print(f"Deleted {result['Deleted Keys Count']} key(s). Non-existent keys: {result['Non-existent Keys']}")
```
# Get Key Stats
```
stats = redis_handler.get_key_stats('my_dataframe_key')
print(stats)
```
# Contributing
Contributions are welcome! If you have suggestions for improvements or new features, please feel free to submit a pull request or open an issue.

# License
This project is licensed under the MIT License. See the LICENSE file for details.

# Contact
If you have any questions or feedback, feel free to reach out via GitHub issues.
