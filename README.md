# Ecommerce Customer Churn — AWS Data Pipeline

## Architecture
S3 (raw CSV) → AWS Glue (PySpark ETL) → S3 (Parquet) → Athena (SQL analytics)

## Dataset
Kaggle: E-commerce Customer Churn (~5,600 rows, 20 columns)
Source: https://www.kaggle.com/datasets/ankitverma2010/ecommerce-customer-churn-analysis-and-prediction

## Stack
AWS S3 · AWS Glue · PySpark · AWS Athena · CloudWatch

## Progress
- [X] Day 1 — S3 setup + raw data upload
- [X] Day 2 — Glue crawler + Data Catalog
- [X] Day 3 — PySpark ETL job
- [ ] Day 4 — Athena SQL analytics
- [ ] Day 5 — Monitoring + documentation
