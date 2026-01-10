from flask import Flask, render_template, request, send_file, abort
from werkzeug.utils import secure_filename
import os
from datetime import datetime

from app.spark.session import getSpark
from app.spark.loader import loadDataset
from app.spark.descriptive import runDescriptiveStats
from app.spark.ml_jobs import runMlJobs


from app.utils.export import ensureDir, saveJson, readJson


app = Flask(__name__)

baseDir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
uploadFolder = os.path.join(baseDir, "data")
resultsFolder = os.path.join(baseDir, "results")

os.makedirs(uploadFolder, exist_ok=True)
os.makedirs(resultsFolder, exist_ok=True)

allowedExtensions = {
    "csv": {".csv"},
    "json": {".json"},
    "text": {".txt"},
}


def isAllowed(fileName: str, fileType: str) -> bool:
    if not fileName or "." not in fileName:
        return False

    ext = "." + fileName.rsplit(".", 1)[1].lower()
    fileType = (fileType or "").lower().strip()

    if fileType not in allowedExtensions:
        return False

    return ext in allowedExtensions[fileType]


def makeRunId(fileName: str) -> str:
    safeName = secure_filename(fileName)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{safeName}"


@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


@app.route("/upload", methods=["GET", "POST"])
def uploadFile():
    if request.method == "GET":
        return render_template("upload.html")

    uploadedFile = request.files.get("file")
    fileType = (request.form.get("file_type") or "csv").lower().strip()

    if not uploadedFile or uploadedFile.filename.strip() == "":
        return render_template("upload.html", error="Please choose a file to upload.")

    fileName = uploadedFile.filename

    if not isAllowed(fileName, fileType):
        allowedList = ", ".join(sorted(allowedExtensions.get(fileType, [])))
        return render_template(
            "upload.html",
            error=f"Invalid file type. For {fileType.upper()} allowed: {allowedList}"
        )

    safeName = secure_filename(fileName)
    savedPath = os.path.join(uploadFolder, safeName)
    uploadedFile.save(savedPath)

    spark = getSpark()
    df = loadDataset(spark, savedPath, fileType)

    descriptiveStats = runDescriptiveStats(df, fileType=fileType)
    mlResults = runMlJobs(df, fileType=fileType)

    runId = makeRunId(fileName)
    runDir = os.path.join(resultsFolder, runId)

    ensureDir(runDir)

    saveJson(os.path.join(runDir, "descriptive.json"), descriptiveStats)
    saveJson(os.path.join(runDir, "ml.json"), mlResults)

    return render_template(
        "result.html",
        runId=runId,
        fileName=fileName,
        fileType=fileType,
        rows=descriptiveStats.get("rows"),
        cols=descriptiveStats.get("cols"),
        columnTypes=descriptiveStats.get("columnTypes", []),
        numericStats=descriptiveStats.get("numericStats", []),
        distinctInfo=descriptiveStats.get("distinctInfo", []),
        nullInfo=descriptiveStats.get("nullInfo", []),
        mlResults=mlResults,
    )


@app.route("/download/<runId>/<which>", methods=["GET"])
def downloadJson(runId: str, which: str):
    which = (which or "").lower().strip()
    if which not in {"descriptive", "ml"}:
        abort(404)

    runDir = os.path.join(resultsFolder, runId)
    filePath = os.path.join(runDir, f"{which}.json")

    if not os.path.exists(filePath):
        abort(404)

    return send_file(filePath, as_attachment=True)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)