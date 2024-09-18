import json
import pandas as pd
import redis
from datetime import datetime
import pytz
import logging

# Configure logger
logger = logging.getLogger(__name__)

class RedisDataHandler:
    """
    The RedisDataHandler class manages interactions with a Redis database, including storing,
    retrieving, and publishing various types of data. It is designed to handle data in multiple 
    formats such as DataFrames, JSON-serialized objects, and plain strings. The class provides 
    flexibility with an option to use Redis pub/sub functionality when required.

    Key functionalities:
    - Publish new rows of a DataFrame to Redis lists and optionally publish via Redis pub/sub.
    - Store and publish JSON-serialized data or plain strings to Redis.
    - Retrieve data stored in Redis in different formats, including DataFrames, JSON, and strings.
    - Manage Redis keys, including deleting keys and getting statistics.
    """
    
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        """
        Initialize the RedisDataHandler with the specified Redis connection details.
        """
        if password:
            self.r = redis.Redis(host=host, port=port, db=db, password=password)
        else:
            self.r = redis.Redis(host=host, port=port, db=db)
        self.est = pytz.timezone('US/Eastern')
        self.today = datetime.now(self.est).strftime("%Y%m%d")
        logger.debug(f"Connected to Redis at {host}:{port}, db: {db}")

    def publish_dataframe(self, df, key, last_published_index, publish=True):
        """
        Publish new rows from a DataFrame to Redis. Optionally publish via Redis pub/sub.

        Parameters:
        - df: The DataFrame to publish.
        - key: The Redis key where the DataFrame rows will be stored.
        - last_published_index: The index of the last published row.
        - publish: If True, also publish to Redis pub/sub. Default is True.
        """
        new_rows = df.iloc[last_published_index + 1:]
        for _, row in new_rows.iterrows():
            row_json = row.to_json()
            self.r.rpush(key, row_json)
            logger.debug(f"Stored new row for {key} in Redis: {row_json}")
            if publish:
                self.r.publish(key, row_json)
                logger.debug(f"Published to Redis under key {key}: {row_json}")
        return len(df) - 1

    def publish_to_redis_json(self, data, key, publish=True):
        """
        Store JSON data in Redis and optionally publish via Redis pub/sub.

        Parameters:
        - data: The data to store and publish (will be serialized to JSON).
        - key: The Redis key where the data will be stored.
        - publish: If True, also publish to Redis pub/sub. Default is True.
        """
        json_data = json.dumps(data)
        self.r.set(key, json_data)
        logger.debug(f"Stored JSON data in Redis under key {key}: {json_data}")
        if publish:
            self.r.publish(key, json_data)
            logger.debug(f"Published to Redis under key {key}: {json_data}")

    def publish_to_redis_str(self, data, key, publish=True):
        """
        Store a string in Redis and optionally publish via Redis pub/sub.

        Parameters:
        - data: The string to store and publish.
        - key: The Redis key where the string will be stored.
        - publish: If True, also publish to Redis pub/sub. Default is True.
        """
        self.r.set(key, str(data))
        logger.debug(f"Stored string data in Redis under key {key}: {data}")
        if publish:
            self.r.publish(key, str(data))
            logger.debug(f"Published to Redis under key {key}: {data}")
        qb = self.r.get(key)
        logger.debug(f"redis_client.get({key}) = {qb.decode('utf-8')}")

    def retrieve_dataframe_from_redis(self, key):
        """
        Retrieve data from a Redis list and convert it into a DataFrame.

        Parameters:
        - key: The Redis key where the DataFrame rows are stored.

        Returns:
        - DataFrame: A DataFrame created from the Redis list.
        """
        data = self.r.lrange(key, 0, -1)  # Get all elements from the list
        rows = [json.loads(row.decode('utf-8')) for row in data]  # Convert JSON strings to dictionaries
        df = pd.DataFrame(rows)  # Create a DataFrame from the dictionaries
        df['timestamp'] = pd.to_datetime(df['timestamp'])  # Convert timestamps
        df = df.drop_duplicates(subset=['timestamp'], keep='last')  # Remove duplicates
        df = df.sort_values(by='timestamp')  # Sort by timestamp
        return df

    def retrieve_json_from_redis(self, key):
        """
        Retrieve and deserialize JSON data from Redis.

        Parameters:
        - key: The Redis key where the JSON data is stored.

        Returns:
        - The deserialized JSON data (e.g., dictionary or list).
        """
        json_data = self.r.get(key)
        if json_data is None:
            return None
        return json.loads(json_data.decode('utf-8'))

    def retrieve_str_from_redis(self, key):
        """
        Retrieve string data from Redis.

        Parameters:
        - key: The Redis key where the string is stored.

        Returns:
        - The string stored under the key.
        """
        data = self.r.get(key)
        if data is None:
            return None
        return data.decode('utf-8')

    def delete_keys(self, keys):
        """
        Delete a single key or a set of keys from Redis.

        Parameters:
        - keys: str or list of str. A single key or a list of keys to delete.

        Returns:
        - dict: A dictionary containing the number of deleted keys and a list of non-existent keys.
        """
        if isinstance(keys, str):
            keys = [keys]

        non_existent_keys = []
        deleted_count = 0

        for key in keys:
            if self.r.exists(key):
                self.r.delete(key)
                deleted_count += 1
                logger.debug(f"Deleted key from Redis: {key}")
            else:
                non_existent_keys.append(key)
                logger.debug(f"Key does not exist in Redis: {key}")

        return {
            'Deleted Keys Count': deleted_count,
            'Non-existent Keys': non_existent_keys
        }

    def get_all_keys(self):
        """
        Get a list of all keys in the Redis database.
        """
        return self.r.keys('*')

    def get_keys_dataframe(self):
        """
        Get a DataFrame containing statistics for all keys in the Redis database.
        """
        all_keys = self.get_all_keys()
        data = []
        for key in all_keys:
            key_str = key.decode('utf-8')
            stats = self.get_key_stats(key_str)
            data.append((key_str, stats['Type'], stats['Number of Items'], stats['Size (Bytes)'], stats['Size (MB)']))

        df = pd.DataFrame(data, columns=['Key', 'Type', 'Number of Items', 'Size (Bytes)', 'Size (MB)'])
        df = df.sort_values(by='Size (MB)', ascending=True)
        return df

    def get_key_stats(self, key):
        """
        Get detailed statistics for a specific Redis key.

        Parameters:
        - key: The Redis key for which to get stats.

        Returns:
        - dict: A dictionary containing the type, number of items, size in bytes, and size in megabytes.
        - str: An error message if the key does not exist.
        """
        if not self.r.exists(key):
            return f"Key '{key}' does not exist in Redis."

        key_type = self.r.type(key).decode('utf-8')
        stats = {
            'Key': key,
            'Type': key_type,
            'Number of Items': 0,
            'Size (Bytes)': 0,
            'Size (MB)': 0.0
        }

        if key_type == 'string':
            size_bytes = self.r.strlen(key)
            stats['Number of Items'] = 1
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        elif key_type == 'list':
            num_items = self.r.llen(key)
            size_bytes = sum(len(item) for item in self.r.lrange(key, 0, -1))
            stats['Number of Items'] = num_items
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        elif key_type == 'set':
            num_items = self.r.scard(key)
            size_bytes = sum(len(member) for member in self.r.smembers(key))
            stats['Number of Items'] = num_items
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        elif key_type == 'zset':
            num_items = self.r.zcard(key)
            size_bytes = sum(len(member) for member, _ in self.r.zscan_iter(key))
            stats['Number of Items'] = num_items
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        elif key_type == 'hash':
            num_items = self.r.hlen(key)
            size_bytes = sum(len(field) + len(value) for field, value in self.r.hgetall(key).items())
            stats['Number of Items'] = num_items
            stats['Size (Bytes)'] = size_bytes
            stats['Size (MB)'] = size_bytes / (1024 * 1024)

        logger.debug(f"Key stats for {key}: {stats}")
        return stats

