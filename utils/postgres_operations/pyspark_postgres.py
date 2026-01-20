import builtins
import math
from pyspark.sql import SparkSession

# Initialize Spark session (only available for this file)
# spark_postgres = SparkSession.builder.appName("ValidationTest").getOrCreate()

def read_from_postgres(table_name=None, query=None, spark=None, connection_details=None):
    """
    Reads data from a PostgreSQL table into a PySpark DataFrame.
    
    :param table_name: The table name (schema.table) to read from.
    :param query: SQL query to execute.
    :param spark: SparkSession object.
    :param connection_details: Dictionary with database connection details.
    :return: PySpark DataFrame containing the data.
    :raises ValueError: If neither query nor table_name is provided.
    """
    
    if not spark:
        raise ValueError("SparkSession is required.")

    if connection_details is None:
        raise ValueError("Connection details must be provided.")

    if not table_name and not query:
        raise ValueError("Either 'table_name' or 'query' must be specified.")

    if table_name and query:
        raise Warning("Preferring Query provided instead of table name")

    jdbc_options = {
        "url": connection_details["jdbc_url"],
        "user": connection_details["user"],
        "password": connection_details["password"],
        "driver": "org.postgresql.Driver",
        "fetchsize": connection_details.get("batchsize", 10000),
        "numPartitions": connection_details.get("numPartitions", 100)
    }

    if query:
        jdbc_options["query"] = query
    else:
        jdbc_options["dbtable"] = table_name

    return spark.read.format("jdbc").options(**jdbc_options).load()


def write_to_postgres(df, table_name, mode="append", connection_details=None,  spark=None):
    """
    Writes a PySpark DataFrame to a PostgreSQL table.

    :param df: PySpark DataFrame to be written.
    :param table_name: The target PostgreSQL table (schema.table).
    :param mode: Write mode - "overwrite", "append", "ignore", or "error".
    :param connection_details: Dictionary with database connection details.
    :raises ValueError: If connection details are not provided.
    """

    if connection_details is None:
        raise ValueError("Connection details must be provided.")
    try:
        batch_size = connection_details.get("batchsize", 100000)
        row_count = df.count()

        total_cores = int(spark._jsc.sc().defaultParallelism())
        ideal_partitions = builtins.max(math.ceil(row_count / batch_size), total_cores * 2)

        df = df.repartition(ideal_partitions)

        df.write \
            .format("jdbc") \
            .option("url", connection_details['jdbc_url']) \
            .option("dbtable", table_name) \
            .option("user", connection_details['user']) \
            .option("password", connection_details['password']) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", batch_size) \
            .option("numPartitions", ideal_partitions) \
            .option("isolationLevel", "READ_COMMITTED") \
            .mode(mode) \
            .save()
        return "Write successful"
    except Exception as e:
        return "Error while writing: " + str(e)