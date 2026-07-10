# HealthPredict-AI
Dibuat untuk latihan lksn 2026
Struktur folder

healthpredict/
├── data/
│   └── diabetes.csv              # Section 6 — upload ke S3 raw/
├── glue/
│   └── healthpredict_etl.py      # Section 6.3 — Glue ETL Job
├── sagemaker/
│   ├── processing_script.py      # Section 10.2 Step 1
│   ├── sagemaker_pipeline.py     # Section 10 — registrasi 3-step pipeline
│   └── deploy_endpoint.py        # Section 11.3 — deploy real-time endpoint
├── sql/
│   └── redshift_schema.sql       # Section 8.1 — 4 tabel, 3 view, 1 SP
├── lambda/
│   ├── predict.py                # Section 14.3
│   ├── history.py                # Section 14.4
│   └── health.py                 # Section 14.5
└── frontend/
    ├── index.html
    ├── results.html
    ├── history.html
    └── config.js                 # Section 15 — Amplify frontend

Cara pakai — ikuti urutan 18 task di Section 1.2

1–5. Networking, Storage, Secrets, DynamoDB/SNS/API Gateway skeleton
Semua resource ini dibuat manual lewat AWS Console mengikuti tabel
spesifikasi di Section 3, 4, 5, 9 — tidak butuh source code, cukup
ikuti nilai parameter di modul (nama harus prefix healthpredict-).


Upload dataset & jalankan Crawler
Upload data/diabetes.csv ke s3://healthpredict-dataset-[namamu]-2026/raw/diabetes.csv,
lalu jalankan crawler healthpredict-crawler-raw.
Glue ETL Job

Ganti [studentname] di nama bucket sesuai punyamu.
Upload glue/healthpredict_etl.py ke s3://healthpredict-glue-[namamu]-2026/scripts/.
Buat Job dengan parameter di Section 6.4–6.5, lalu Run.
Catat nilai mean/std yang di-print di CloudWatch Logs job ini —
nilai itu perlu kamu masukkan ke FEATURE_STATS di lambda/predict.py
supaya normalisasi di Lambda konsisten dengan yang dipakai saat training.



Redshift schema
Jalankan seluruh isi sql/redshift_schema.sql di Query Editor v2.
Athena — jalankan query di Section 7.3 (sudah ada di naskah soal).


10–11. SageMaker Pipeline


Edit STUDENT_NAME di sagemaker/sagemaker_pipeline.py.
Upload sagemaker_pipeline.py + processing_script.py ke Studio, lalu
jalankan python sagemaker_pipeline.py dari terminal Studio (ini hanya
mendaftarkan pipeline, belum start execution).
Start execution dari Studio UI (Pipelines panel) dan tunggu sampai
3 step berstatus Succeeded.



Model Registry & Endpoint



Approve model version terbaru di Model Registry console.
Jalankan python sagemaker/deploy_endpoint.py dari Studio terminal.



Batch Transform
Jalankan lewat SageMaker Console/boto3 sesuai parameter di Section 12.2
(model source = versi Approved terbaru).
EventBridge
Buat rule sesuai Section 13.2 — cukup lewat Console, target-nya pipeline ARN.
Lambda & API Gateway



Deploy lambda/predict.py, lambda/history.py, lambda/health.py
sebagai 3 fungsi terpisah sesuai runtime/memory/timeout di Section 14.1.
predict.py dan history.py butuh layer psycopg2 untuk koneksi
Redshift (bisa pakai public Lambda layer psycopg2, atau build sendiri
karena AWS Academy tidak selalu punya akses ke semua layer publik).
Set semua environment variable sesuai Section 14.2.
Buat API Gateway dengan 4 route sesuai tabel Section 14.5, aktifkan API Key
dan Usage Plan untuk 3 route yang perlu auth.



Amplify frontend



Edit frontend/config.js — isi INVOKE_URL dan API_KEY dari API Gateway
yang sudah kamu deploy.
Zip 4 file di folder frontend/ (index.html, results.html, history.html,
config.js) jadi healthpredict-frontend.zip — file harus di root ZIP,
bukan di dalam subfolder.
Upload lewat Amplify Console → "Deploy without Git provider".



Verifikasi end-to-end
Ikuti urutan di Section 16.1 (Secrets → Glue+Athena → Pipeline+Registry →
Batch Transform → API dual-write → EventBridge).


Catatan penting untuk latihan LKS


Waktu: modul asli 3-4 jam. Latih diri kerjakan networking (VPC dkk) di
bawah 30-40 menit karena itu paling banyak field yang harus diisi manual.
LabRole: AWS Academy Learner Lab tidak bisa membuat IAM Role baru —
semua resource harus pakai LabRole yang sudah ada, jangan coba buat role baru.
Hapus endpoint SageMaker setelah testing — biaya jalan terus selama
endpoint InService, ini sering jadi poin yang kelupaan pas lomba.
psycopg2 Lambda layer sering jadi blocker di AWS Academy karena
keterbatasan region/service. Siapkan solusi ini duluan saat latihan supaya
tidak kehabisan waktu pas hari-H.
File-file ini adalah starting point yang mengikuti spesifikasi modul
persis — kamu tetap perlu mengetik/upload manual di Console sesuai urutan
18 task, karena itulah yang dinilai di kompetisi (bukan cuma py/sql-nya jadi).
