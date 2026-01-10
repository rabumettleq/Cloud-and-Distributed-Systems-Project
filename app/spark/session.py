from pyspark.sql import SparkSession

sparkInstance = None


def getSpark(appName: str = "CloudDataProcessing"):
    global sparkInstance

    if sparkInstance is None:
        sparkInstance = (
            SparkSession.builder
            .appName(appName)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

    return sparkInstance
