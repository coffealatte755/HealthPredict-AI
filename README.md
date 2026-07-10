# HealthPredict-AI
# 🩺 HealthPredict-AI

HealthPredict-AI merupakan project latihan untuk **LKSN Cloud Computing 2026** yang mensimulasikan implementasi machine learning end-to-end menggunakan layanan AWS.

---

# 📂 Project Structure

```text
healthpredict/
├── data/
│   └── diabetes.csv              # Dataset (Section 6)
│
├── glue/
│   └── healthpredict_etl.py      # Glue ETL Job
│
├── sagemaker/
│   ├── processing_script.py      # SageMaker Processing
│   ├── sagemaker_pipeline.py     # Pipeline Registration
│   └── deploy_endpoint.py        # Endpoint Deployment
│
├── sql/
│   └── redshift_schema.sql       # Redshift Schema
│
├── lambda/
│   ├── predict.py                # Prediction API
│   ├── history.py                # Prediction History API
│   └── health.py                 # Health Check API
│
└── frontend/
    ├── index.html
    ├── results.html
    ├── history.html
    └── config.js
```

---

# 🚀 Deployment Flow

Ikuti seluruh task pada **Section 1.2** sesuai urutan.

## ✅ Task 1–5

### Infrastructure

Buat resource berikut melalui AWS Console.

- VPC
- Subnet
- Route Table
- Internet Gateway
- NAT Gateway
- Security Group

### Storage

- S3 Bucket
- Secrets Manager

### Backend Service

- DynamoDB
- SNS
- API Gateway Skeleton

> Semua resource menggunakan prefix:

```text
healthpredict-
```

---

# 📊 Dataset & Glue ETL

## Upload Dataset

```text
data/diabetes.csv
```

Upload ke

```text
s3://healthpredict-dataset-[student]-2026/raw/diabetes.csv
```

Kemudian jalankan

```
healthpredict-crawler-raw
```

---

## Glue ETL

Upload script

```text
glue/healthpredict_etl.py
```

ke

```text
s3://healthpredict-glue-[student]-2026/scripts/
```

Lalu

- Create Glue Job
- Configure parameter sesuai Section 6
- Run Job

### Penting

Catat hasil berikut pada CloudWatch Logs

```python
mean
std
```

Nilai tersebut harus dimasukkan ke

```python
FEATURE_STATS
```

di file

```text
lambda/predict.py
```

agar normalisasi saat inference sama dengan proses training.

---

# 🗄 Redshift & Athena

## Redshift

Jalankan

```text
sql/redshift_schema.sql
```

yang berisi

- 4 Tables
- 3 Views
- 1 Stored Procedure

---

## Athena

Jalankan query pada

**Section 7.3**

---

# 🤖 SageMaker Pipeline

Edit terlebih dahulu

```python
STUDENT_NAME
```

di

```text
sagemaker/sagemaker_pipeline.py
```

Upload ke SageMaker Studio

```
processing_script.py
sagemaker_pipeline.py
```

Lalu jalankan

```bash
python sagemaker_pipeline.py
```

Selanjutnya

- Open Pipelines
- Start Execution
- Tunggu seluruh 3 step menjadi

```text
Succeeded
```

---

# 📦 Model Registry & Endpoint

1. Approve model terbaru pada Model Registry.

2. Deploy endpoint.

```bash
python deploy_endpoint.py
```

---

# 📈 Batch Transform

Jalankan Batch Transform menggunakan

- Model Version terbaru
- Status Approved

---

# ⏰ EventBridge

Buat EventBridge Rule

Target

```text
SageMaker Pipeline ARN
```

---

# ⚡ Lambda & API Gateway

Deploy tiga Lambda berikut.

```
predict.py
history.py
health.py
```

Konfigurasi sesuai Section 14.

## Dependency

Gunakan layer

```
psycopg2
```

untuk

- predict.py
- history.py

agar dapat terhubung ke Amazon Redshift.

---

## Environment Variables

Isi seluruh Environment Variable sesuai Section 14.2.

---

## API Gateway

Buat 4 endpoint.

Aktifkan

- API Key
- Usage Plan

untuk endpoint yang membutuhkan autentikasi.

---

# 🌐 Amplify Frontend

Edit

```javascript
frontend/config.js
```

Isi

```javascript
INVOKE_URL
API_KEY
```

Kemudian zip file berikut

```
index.html
results.html
history.html
config.js
```

Menjadi

```text
healthpredict-frontend.zip
```

> Pastikan file berada di root ZIP, bukan di dalam folder.

Deploy melalui

```
Amplify Console
→ Deploy without Git Provider
```

---

# ✅ End-to-End Verification

Lakukan pengujian secara berurutan.

- Secrets Manager
- Glue
- Athena
- SageMaker Pipeline
- Model Registry
- Batch Transform
- API Gateway
- EventBridge

---

# ⚠️ Important Notes

## Waktu Pengerjaan

Target latihan

```
3–4 Jam
```

Target Networking

```
30–40 Menit
```

---

## AWS Academy

Gunakan

```
LabRole
```

Jangan membuat IAM Role baru karena tidak diizinkan pada AWS Academy Learner Lab.

---

## SageMaker Endpoint

Setelah selesai testing

```
Delete Endpoint
```

karena endpoint tetap dikenakan biaya selama status **InService**.

---

## psycopg2

Layer psycopg2 sering menjadi kendala pada AWS Academy.

Disarankan menyiapkan:

- Public Layer
- Custom Layer

sebelum memulai latihan.

---

# 🎯 Tujuan Repository

Repository ini digunakan sebagai **starting point** latihan **LKSN Cloud Computing 2026**.

Walaupun source code telah disediakan, seluruh resource AWS tetap harus dibuat secara manual melalui AWS Console sesuai spesifikasi modul karena proses tersebut menjadi bagian dari penilaian kompetisi.
