# Cloud-Based Distributed Data Processing Service

## Project Description

This project implements a cloud-based distributed data processing service using **Flask** and **PySpark**.
The system allows users to upload large datasets and execute distributed data processing and machine learning jobs.

The project is executed on **Google Colab** as a cloud simulation environment, where Spark runs in multi-worker mode:
local[1], local[2], local[4], local[8]

This simulates a distributed cluster and enables performance evaluation using **Speedup** and **Efficiency**.

---

## Technologies

* Backend: Flask
* Distributed Engine: PySpark
* Cloud Simulation: Google Colab
* Storage: Google Drive
* Frontend: HTML, CSS

---

## Features

* Upload dataset (CSV, JSON, TXT)
* Load large datasets directly from Google Drive
* Descriptive statistics using Spark
* Machine Learning jobs:

  * Regression
  * Classification
  * Clustering
  * Time-Series aggregation
* Distributed performance testing using:

  * 1 Worker
  * 2 Workers
  * 4 Workers
  * 8 Workers
* Speedup and Efficiency calculation

---

## Execution Environment

The project runs on **Google Colab** and simulates a distributed cluster using Spark local workers.

Each job is executed multiple times with different numbers of workers to evaluate distributed performance.

---

## How to Run

1. Clone the project:

!git clone https://github.com/rabumettleq/Cloud-and-Distributed-Systems-Project.git
%cd Cloud-and-Distributed-Systems-Project

2. Install dependencies:

!pip install -r requirements.txt

3. Mount Google Drive:

from google.colab import drive
drive.mount('/content/drive')
!pip -q install gdown
!mkdir -p /content/drive/MyDrive/datasets
!gdown --fuzzy "https://drive.google.com/file/d/1zN4e2vv1MH11V15YJ2w6T4vxO5ZJ8dhu/view?usp=sharing" -O "/content/drive/MyDrive/datasets/big.csv"

4. Install pyngrok:

!pip -q install pyngrok

5. Connect ngrok with auth token:

from pyngrok import ngrok
ngrok.set_auth_token("386kuXLhn554ElmLslX5BqcfAfL_6BuAW5ffTGg3Y3s43fNa8")

6. Run the Flask application:

python -m app.app

7. Run ngrok To Get the link of the app

from pyngrok import ngrok
public_url = ngrok.connect(5000, "http")
print(public_url)

---

## Project Repository
GitHub:
[https://github.com/rabumettleq/Cloud-and-Distributed-Systems-Project]