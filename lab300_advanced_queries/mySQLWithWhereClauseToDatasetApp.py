"""
 MySQL injection to Spark, using the Sakila sample database.
 @author Dmitry Ivakin
"""

import logging
from pyspark.sql import SparkSession
from config.config import config as cfg

# настраиваем журналирование
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s"
)

# строку подключения забираем из ini-файла
prms = cfg(filename="../database.ini",section="mysql")

jdbc_read_config = {
    "url": prms["url"],
    "driver": prms["driver"],
    "user": prms["user"],
    "password": prms["password"] ,
    "numPartitions": 10
}

def get_dataframe_by_query(sc:SparkSession, jdbc_config: dict, query: str):
    return spark.read.format("jdbc")\
        .options(**jdbc_read_config)\
        .option("query",query)\
        .load()

sqlQuery0 = """
    select * from film where 
    (title like \"%ALIEN%\" or title like \"%victory%\" 
    or title like \"%agent%\" or description like \"%action%\") 
    and rental_rate>1 
    and (rating=\"G\" or rating=\"PG\")
"""

sqlQuery1 = """
    select actor.first_name, actor.last_name, film.title, 
    film.description 
    from actor, film_actor, film 
    where actor.actor_id = film_actor.actor_id 
    and film_actor.film_id = film.film_id
"""

sqlQuery2 = """
    select 
        a.first_name, 
        a.last_name, 
        f.title,
        f.description  
    from actor a
    inner join film_actor fa on a.actor_id = fa.actor_id
    inner join film f on f.film_id = fa.film_id
"""

def schema_to_json(df):
    import json
    schemaAsJson = df.schema.json()
    parsedSchemaAsJson = json.loads(schemaAsJson)
    return "*** Schema as JSON: {0}".format(json.dumps(parsedSchemaAsJson, indent=2))


if __name__ == '__main__':

    # Creates a session on a local master
    spark = SparkSession.builder\
        .appName("MySQL with where clause to Dataframe using a JDBC Connection") \
        .master("local[*]")\
        .getOrCreate()

    # это плохо - см. ниже через функцию
    df = spark.read \
        .format("jdbc") \
        .option("url", prms["url"]) \
        .option("driver", prms["driver"]) \
        .option("user", prms["user"]) \
        .option("password", prms["password"]) \
        .option("query",sqlQuery0) \
        .option("numPartitions", 10)\
        .load()

    # Displays the dataframe and some of its metadata
    df.show(5)
    logging.info("The dataframe contains {} record(s).".format(df.count()))
    logging.info("The dataframe is split over ${} partition(s).".format(df.rdd.getNumPartitions()))
    logging.info(schema_to_json(df))

    df1 = get_dataframe_by_query(spark,jdbc_read_config,sqlQuery1)
    df1.show(5)
    logging.info("The dataframe contains {} record(s).".format(df1.count()))
    logging.info("The dataframe is split over ${} partition(s).".format(df1.rdd.getNumPartitions()))
    logging.info(schema_to_json(df1))

    df2 = get_dataframe_by_query(spark,jdbc_read_config,sqlQuery2)
    df2.show(5)
    logging.info("The dataframe contains {} record(s).".format(df2.count()))
    logging.info("The dataframe is split over ${} partition(s).".format(df2.rdd.getNumPartitions()))
    logging.info(schema_to_json(df2))

    spark.stop()
