import json
import redis
from pymemcache.client import base
import psycopg2
import re

json_file = 'parsed_NASA_access_log.json'

# Database configurations
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
redis_simple_client = redis.StrictRedis(host='localhost', port=6378, db=1)
memcached_client = base.Client(('localhost', 11211))

pg_conn = psycopg2.connect(
    dbname="your_postgres_db",
    user="your_postgres_user",
    password="your_postgres_password",
    host="localhost",
    port=5432
)
pg_cursor = pg_conn.cursor()

# Sanitize key for Memcached
def sanitize_key(key):
    return re.sub(r'\s+', '_', key)

def load_data_to_in_memory_db():
    with open(json_file, 'r') as file:
        data = json.load(file)

        for entry in data:
            unique_key = f"{entry['host']}:{entry['timestamp']}"
            sanitized_key = sanitize_key(unique_key)

            entry_json = json.dumps(entry)

            redis_client.hset(sanitized_key, mapping=entry)

            redis_simple_client.set(sanitized_key, entry_json)

            memcached_client.set(sanitized_key, entry_json)

            insert_query = """
                INSERT INTO nasa_logs (host, timestamp, request, status, bytes_sent)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """
            pg_cursor.execute(
                insert_query,
                (
                    entry['host'],
                    entry['timestamp'],
                    entry['request'],
                    entry['status'],
                    entry['bytes_sent']
                )
            )

        pg_conn.commit()

    print("Data successfully loaded into Redis, Memcached, and PostgreSQL!")

def create_pg_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS nasa_logs (
        id SERIAL PRIMARY KEY,
        host TEXT,
        timestamp TEXT,
        request TEXT,
        status INTEGER,
        bytes_sent INTEGER
    );
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()

create_pg_table()
load_data_to_in_memory_db()

pg_cursor.close()
pg_conn.close()