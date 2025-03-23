import json
import sys
import logging
import redis
import pymysql
import time


# TODO 1
DB_HOST = "uiuc-cca-ayush-mp6-db.cluster-c2nkmcoy89z2.us-east-1.rds.amazonaws.com"  # Add DB endpoint
DB_USER = "admin"  # Add your database user
DB_PASS = "Ashsenju99$"  # Add your database password
DB_NAME = "superheroes_db"  # Add your database name
DB_TABLE = "superheroes"  # Add your table name
REDIS_URL = "rediss://master.uiuc-cca-ayush-mp6-redis.vhm9un.use1.cache.amazonaws.com:6379"  # Add redis endpoint as URL
logger = logging.getLogger()
logger.setLevel(logging.INFO)

TTL = 60

class DB:
    def __init__(self, **params):
        params.setdefault("charset", "utf8mb4")
        params.setdefault("cursorclass", pymysql.cursors.DictCursor)

        self.mysql = pymysql.connect(**params)

    def query(self, sql):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def record(self, sql, values):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql, values)
            return cursor.fetchone()

    def get_idx(self, table_name):
        with self.mysql.cursor() as cursor:
            cursor.execute(f"SELECT MAX(id) as id FROM {table_name}")
            idx = str(cursor.fetchone()['id'] + 1)
            return idx

    def insert(self, idx, data, table_name):
        with self.mysql.cursor() as cursor:
            hero = data["hero"]
            power = data["power"]
            name = data["name"]
            xp = data["xp"]
            color = data["color"]
            
            sql = f"INSERT INTO {table_name} (`id`, `hero`, `power`, `name`, `xp`, `color`) VALUES ('{idx}', '{hero}', '{power}', '{name}', '{xp}', '{color}')"
            cursor.execute(sql)
            self.mysql.commit()

# TODO 2: Implement Lazy Loading strategy for read with ordered output.
def read(use_cache, xps, Database, Cache):
    result = []
    start_time = time.time()
    redis_time = 0
    db_time = 0
    
    if use_cache:
        # Build a list of keys for all requested xp values (converted to int for consistency)
        keys = ["xp:" + str(int(xp)) for xp in xps]
        # Batch retrieve all keys in one call
        redis_start = time.time()
        cached_vals = Cache.mget(keys)
        redis_time += time.time() - redis_start
        # Create a pipeline for batch SET commands for any cache misses
        pipe = Cache.pipeline()
        for i, xp in enumerate(xps):
            xp_int = int(xp)
            if cached_vals[i] is not None:
                logger.info(f"Cache hit for xp {xp_int}")
                row = json.loads(cached_vals[i])
            else:
                logger.info(f"Cache miss for xp {xp_int}, querying DB")
                db_start = time.time()
                db_row = Database.record(f"SELECT * FROM {DB_TABLE} WHERE xp = %s", (xp_int,))
                db_time += time.time() - db_start
                if db_row:
                    row = {
                        "id": db_row["id"],
                        "name": db_row["name"],
                        "hero": db_row["hero"],
                        "power": db_row["power"],
                        "xp": db_row["xp"],
                        "color": db_row["color"]
                    }
                    # Queue the cache update in the pipeline
                    pipe.set("xp:" + str(xp_int), json.dumps(row), ex=TTL)
                else:
                    row = None
            result.append(row)
        # Execute all pending SET commands in one go
        redis_start = time.time()
        pipe.execute()
        redis_time += time.time() - redis_start
    else:
        for xp in xps:
            xp_int = int(xp)
            logger.info(f"Querying DB for xp {xp_int} without cache")
            db_start = time.time()
            db_row = Database.record(f"SELECT * FROM {DB_TABLE} WHERE xp = %s", (xp_int,))
            db_time += time.time() - db_start
            if db_row:
                row = {
                    "id": db_row["id"],
                    "name": db_row["name"],
                    "hero": db_row["hero"],
                    "power": db_row["power"],
                    "xp": db_row["xp"],
                    "color": db_row["color"]
                }
            else:
                row = None
            result.append(row)
    
    elapsed = time.time() - start_time
    logger.info(f"Read operation took: {elapsed:.4f}s, Redis time: {redis_time:.4f}s, DB time: {db_time:.4f}s")
    return result

# TODO 3: Write function remains as before
def write(use_cache, sqls, Database, Cache):
    start_time = time.time()
    for data in sqls:
        logger.info("Processing write for data: " + json.dumps(data))
        idx = Database.get_idx(DB_TABLE)
        Database.insert(idx, data, DB_TABLE)
        if use_cache:
            key = "xp:" + str(data["xp"])
            Cache.set(key, json.dumps(data), ex=TTL)
            logger.info(f"Cache updated for key {key}")
    end_time = time.time()
    elapsed = end_time - start_time
    logger.info(f"Write operation took: {elapsed:.4f} seconds")

Cache = redis.Redis.from_url(REDIS_URL, max_connections=10)
def lambda_handler(event, context):
    
    USE_CACHE = (event['USE_CACHE'] == "True")
    REQUEST = event['REQUEST']
    
    # Initialize database
    try:
        Database = DB(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME)
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
        sys.exit()
        
    # Initialize cache
    #Cache = redis.Redis.from_url(REDIS_URL)
    
    result = []
    if REQUEST == "read":
        # event["SQLS"] is a list of all xps for which you have to query the database or redis.
        result = read(USE_CACHE, event["SQLS"], Database, Cache)
        
    elif REQUEST == "write":
        # event["SQLS"] should be a list of jsons. You have to write these rows to the database.
        write(USE_CACHE, event["SQLS"], Database, Cache)
        result = "write success"
    
    
    return {
        'statusCode': 200,
        'body': result
    }
