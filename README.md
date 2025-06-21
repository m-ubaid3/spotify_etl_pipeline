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

## 🖼️ Architecture Diagram

*(Add your diagram here — upload to `resources/` and link it below)*

![Architecture](resources/architecture-diagram.png)

---

## 🧪 Sample SQL Queries

```sql
--Get top 10 most popular songs
SELECT song_name, popularity, artist_name
FROM songs_data
ORDER BY popularity DESC
LIMIT 10;

-- Count of songs per artist
SELECT artist_name, COUNT(*) AS song_count
FROM songs_data
GROUP BY artist_name
ORDER BY song_count DESC;

-- Songs released after 2020
SELECT song_name, release_date
FROM songs_data
WHERE release_date >= '2020-01-01'
ORDER BY release_date DESC;

--Find artist details by name
SELECT *
FROM artists_data
WHERE artist_name ILIKE '%weeknd%';


