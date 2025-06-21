# 🎵 Spotify ETL Data Pipeline (Serverless on AWS)

A fully automated, serverless ETL pipeline that extracts Spotify playlist data using AWS Lambda, transforms it with Python, and loads it into Amazon Athena for SQL analysis.

---

## 🚀 Overview

This pipeline:
- Extracts song data from Spotify API using AWS Lambda
- Stores raw JSON in Amazon S3
- Transforms raw data to structured CSVs using Pandas in Lambda
- Loads processed files into S3, partitioned by song and artist
- Uses AWS Glue Crawlers to catalog the data
- Queries structured data via Athena using SQL
- Is fully automated using CloudWatch and S3 Event triggers

---

## 🧰 Tech Stack

|  Component        | Tool/Service             |
|-------------------|--------------------------|
| ETL Scripting     | Python, Pandas           |
| Data Ingestion    | AWS Lambda + Spotify API |
| Storage           | Amazon S3                |
| Data Catalog      | AWS Glue Crawlers        |
| Query Engine      | Amazon Athena            |
| Automation        | CloudWatch, S3 Events    |

---

## 🗂️ Project Structure

spotify-etl-pipeline/
│
├── lambda/
│ ├── extract_lambda.py
│ └── transform_lambda.py
│
├── glue/
│ └── crawler_setup_notes.md
│
├── resources/
│ ├── architecture-diagram.png
│ └── sample_output/
│ ├── sample_song.csv
│ └── sample_artist.csv
│
├── README.md


---

## 🖼️ Architecture Diagram

*(Add your diagram here — upload to `resources/` and link it below)*

![Architecture](resources/architecture-diagram.png)

---

## 🧪 Sample SQL Queries

```sql
-- Get top 10 popular songs
SELECT song_name, popularity
FROM songs
ORDER BY popularity DESC
LIMIT 10;

-- Count songs per artist
SELECT artist_name, COUNT(*) AS song_count
FROM songs
GROUP BY artist_name
ORDER BY song_count DESC;


