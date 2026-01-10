from pyspark.sql import DataFrame

def loadDataset(spark, filePath: str, fileType: str) -> DataFrame:
    fileType = (fileType or "").lower().strip()

    if fileType == "csv":
        return spark.read.csv(filePath, header=True, inferSchema=True)

    if fileType == "json":
        return spark.read.json(filePath)

    if fileType == "text":
        return spark.read.text(filePath)

    raise ValueError(f"Unsupported fileType: {fileType}")
