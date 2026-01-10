from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def runDescriptiveStats(df: DataFrame, fileType: str = "csv") -> dict:
    fileType = (fileType or "").lower().strip()


    rows = df.count()
    cols = len(df.columns)

    columnTypes = []
    for colName, colType in df.dtypes:
        columnTypes.append({
            "column": colName,
            "type": colType
        })


    nullInfo = []
    for colName in df.columns:
        nullCount = df.filter(F.col(colName).isNull()).count()
        missingPercent = (nullCount / rows * 100.0) if rows > 0 else 0.0
        nullInfo.append({
            "column": colName,
            "nullCount": int(nullCount),
            "missingPercent": float(round(missingPercent, 4))
        })


    distinctInfo = []
    for colName in df.columns:
        distinctCount = df.select(F.col(colName)).distinct().count()
        distinctInfo.append({
            "column": colName,
            "distinctCount": int(distinctCount)
        })

    numericTypes = {"int", "bigint", "double", "float", "smallint", "tinyint", "decimal", "long", "short"}
    numericCols = [c for c, t in df.dtypes if t.lower() in numericTypes]

    numericStats = []
    if numericCols:
        aggExprs = []
        for c in numericCols:
            aggExprs.append(F.min(F.col(c)).alias(f"{c}__min"))
            aggExprs.append(F.max(F.col(c)).alias(f"{c}__max"))
            aggExprs.append(F.mean(F.col(c)).alias(f"{c}__mean"))

        aggRow = df.agg(*aggExprs).collect()[0].asDict()

        for c in numericCols:
            minVal = aggRow.get(f"{c}__min")
            maxVal = aggRow.get(f"{c}__max")
            meanVal = aggRow.get(f"{c}__mean")

            numericStats.append({
                "column": c,
                "min": None if minVal is None else float(minVal),
                "max": None if maxVal is None else float(maxVal),
                "mean": None if meanVal is None else float(round(meanVal, 6)) if meanVal is not None else None
            })

    return {
        "rows": int(rows),
        "cols": int(cols),
        "columnTypes": columnTypes,
        "nullInfo": nullInfo,
        "distinctInfo": distinctInfo,
        "numericStats": numericStats,
        "fileType": fileType
    }
