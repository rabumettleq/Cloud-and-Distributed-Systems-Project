from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans


def _getNumericCols(df: DataFrame) -> list:
    numericTypes = {"int", "bigint", "double", "float", "smallint", "tinyint", "decimal", "long", "short"}
    return [c for c, t in df.dtypes if t.lower() in numericTypes]


def _getCandidateLabelCols(df: DataFrame) -> list:
    candidates = []
    for c in df.columns:
        try:
            distinctCount = df.select(F.col(c)).na.drop().distinct().count()
            if 2 <= distinctCount <= 20:
                candidates.append((c, distinctCount))
        except Exception:
            continue
    candidates.sort(key=lambda x: x[1])
    return [c for c, _ in candidates]


def _prepareNumericFeatures(df: DataFrame, featureCols: list) -> DataFrame:
    cleanDf = df.dropna(subset=featureCols)
    assembler = VectorAssembler(inputCols=featureCols, outputCol="features")
    return assembler.transform(cleanDf)


def _findDateColumn(df: DataFrame) -> str:
    lowered = [(c, c.lower()) for c in df.columns]
    for c, lc in lowered:
        if "date" in lc or "time" in lc or "timestamp" in lc:
            return c
    for c, t in df.dtypes:
        tl = t.lower()
        if tl in {"date", "timestamp"}:
            return c

    return ""


def _makeTimestampCol(df: DataFrame, dateCol: str):
    colType = None
    for c, t in df.dtypes:
        if c == dateCol:
            colType = t.lower()
            break

    c = F.col(dateCol)
    if colType in {"timestamp"}:
        return c
    if colType in {"date"}:
        return c.cast("timestamp")

    ts = F.to_timestamp(c)
    ts2 = F.to_timestamp(c, "yyyy-MM-dd")
    ts3 = F.to_timestamp(c, "yyyy/MM/dd")
    ts4 = F.to_timestamp(c, "dd/MM/yyyy")
    return F.coalesce(ts, ts2, ts3, ts4)


def runMlJobs(df: DataFrame, fileType: str = "csv") -> dict:
    fileType = (fileType or "").lower().strip()

    if fileType == "text":
        return {
            "fileType": fileType,
            "numericColumns": [],
            "jobs": [
                {"jobName": "regression", "skipped": True, "reason": "Text data is not supported for this ML pipeline."},
                {"jobName": "classification", "skipped": True, "reason": "Text data is not supported for this ML pipeline."},
                {"jobName": "clustering", "skipped": True, "reason": "Text data is not supported for this ML pipeline."},
                {"jobName": "timeSeries", "skipped": True, "reason": "Text data is not supported for time-series aggregation."},
            ]
        }

    numericCols = _getNumericCols(df)
    jobs = []


# 1) Regression (LinearRegression)
    if len(numericCols) < 2:
        jobs.append({
            "jobName": "regression",
            "skipped": True,
            "reason": "Not enough numeric columns (need at least 2: label + feature)."
        })
    else:
        try:
            labelCol = numericCols[-1]
            featureCols = numericCols[:-1]

            cleanDf = df.dropna(subset=featureCols + [labelCol])
            assembler = VectorAssembler(inputCols=featureCols, outputCol="features")
            regDf = assembler.transform(cleanDf).select("features", F.col(labelCol).alias("label"))

            if regDf.count() == 0:
                jobs.append({"jobName": "regression", "skipped": True, "reason": "No rows after removing nulls."})
            else:
                lr = LinearRegression(featuresCol="features", labelCol="label")
                model = lr.fit(regDf)
                summary = model.summary

                jobs.append({
                    "jobName": "regression",
                    "skipped": False,
                    "labelColumn": labelCol,
                    "featureColumns": featureCols,
                    "rmse": float(summary.rootMeanSquaredError),
                    "r2": float(summary.r2),
                    "coefficients": model.coefficients.toArray().tolist(),
                    "intercept": float(model.intercept),
                })
        except Exception as e:
            jobs.append({"jobName": "regression", "skipped": True, "reason": str(e)})


# 2) Classification (LogisticRegression)
    labelCandidates = _getCandidateLabelCols(df)

    if len(numericCols) < 1:
        jobs.append({
            "jobName": "classification",
            "skipped": True,
            "reason": "No numeric feature columns found."
        })
    elif len(labelCandidates) == 0:
        jobs.append({
            "jobName": "classification",
            "skipped": True,
            "reason": "No suitable label column found (need a column with 2..20 distinct values)."
        })
    else:
        try:
            labelCol = None
            for c in labelCandidates:
                if c not in numericCols:
                    labelCol = c
                    break
            if labelCol is None:
                labelCol = labelCandidates[0]

            featureCols = [c for c in numericCols if c != labelCol]

            if len(featureCols) == 0:
                jobs.append({
                    "jobName": "classification",
                    "skipped": True,
                    "reason": "No numeric features left after excluding label column."
                })
            else:
                cleanDf = df.dropna(subset=featureCols + [labelCol])

                indexer = StringIndexer(inputCol=labelCol, outputCol="label", handleInvalid="skip")
                indexedDf = indexer.fit(cleanDf).transform(cleanDf)

                assembledDf = _prepareNumericFeatures(indexedDf, featureCols).select("features", "label")

                if assembledDf.count() == 0:
                    jobs.append({"jobName": "classification", "skipped": True, "reason": "No rows after removing nulls/invalid labels."})
                else:
                    trainDf, testDf = assembledDf.randomSplit([0.8, 0.2], seed=42)

                    if trainDf.count() == 0 or testDf.count() == 0:
                        jobs.append({"jobName": "classification", "skipped": True, "reason": "Not enough data after train/test split."})
                    else:
                        clf = LogisticRegression(featuresCol="features", labelCol="label", maxIter=50)
                        model = clf.fit(trainDf)

                        preds = model.transform(testDf).select("label", "prediction")
                        accuracy = preds.filter(F.col("label") == F.col("prediction")).count() / preds.count()

                        jobs.append({
                            "jobName": "classification",
                            "skipped": False,
                            "labelColumn": labelCol,
                            "featureColumns": featureCols,
                            "accuracy": float(round(accuracy, 6)),
                            "numClasses": int(indexedDf.select("label").distinct().count())
                        })
        except Exception as e:
            jobs.append({"jobName": "classification", "skipped": True, "reason": str(e)})


# 3) Clustering (KMeans)
    if len(numericCols) < 2:
        jobs.append({
            "jobName": "clustering",
            "skipped": True,
            "reason": "Not enough numeric columns (need at least 2)."
        })
    else:
        try:
            vecDf = _prepareNumericFeatures(df, numericCols).select("features")
            if vecDf.count() == 0:
                jobs.append({"jobName": "clustering", "skipped": True, "reason": "No rows after removing nulls."})
            else:
                k = 3
                model = KMeans(k=k, seed=42, featuresCol="features").fit(vecDf)
                centers = [c.toArray().tolist() for c in model.clusterCenters()]
                jobs.append({
                    "jobName": "clustering",
                    "skipped": False,
                    "algorithm": "kmeans",
                    "k": k,
                    "clusterCenters": centers
                })
        except Exception as e:
            jobs.append({"jobName": "clustering", "skipped": True, "reason": str(e)})


# 4) Time-Series Analysis (daily aggregation)
    dateCol = _findDateColumn(df)
    if dateCol == "":
        jobs.append({
            "jobName": "timeSeries",
            "skipped": True,
            "reason": "No date/time column found (try naming it date/time/timestamp)."
        })
    elif len(numericCols) == 0:
        jobs.append({
            "jobName": "timeSeries",
            "skipped": True,
            "reason": "No numeric columns found for aggregation."
        })
    else:
        try:
            valueCol = numericCols[0]
            tsCol = _makeTimestampCol(df, dateCol).alias("ts")

            tsDf = df.select(tsCol, F.col(valueCol).alias("value")).dropna(subset=["ts", "value"])

            if tsDf.count() == 0:
                jobs.append({"jobName": "timeSeries", "skipped": True, "reason": "No rows after parsing timestamps or removing nulls."})
            else:
                dailyAgg = (
                    tsDf
                    .withColumn("day", F.to_date(F.col("ts")))
                    .groupBy("day")
                    .agg(
                        F.count("*").alias("count"),
                        F.mean("value").alias("meanValue"),
                        F.min("value").alias("minValue"),
                        F.max("value").alias("maxValue"),
                        F.sum("value").alias("sumValue"),
                    )
                    .orderBy("day")
                )

                sampleRows = dailyAgg.limit(10).collect()
                sample = []
                for r in sampleRows:
                    sample.append({
                        "day": str(r["day"]),
                        "count": int(r["count"]),
                        "meanValue": None if r["meanValue"] is None else float(r["meanValue"]),
                        "minValue": None if r["minValue"] is None else float(r["minValue"]),
                        "maxValue": None if r["maxValue"] is None else float(r["maxValue"]),
                        "sumValue": None if r["sumValue"] is None else float(r["sumValue"]),
                    })

                jobs.append({
                    "jobName": "timeSeries",
                    "skipped": False,
                    "dateColumn": dateCol,
                    "valueColumn": valueCol,
                    "aggregation": "daily",
                    "sample": sample
                })
        except Exception as e:
            jobs.append({"jobName": "timeSeries", "skipped": True, "reason": str(e)})

    return {
        "fileType": fileType,
        "numericColumns": numericCols,
        "jobs": jobs
    }
