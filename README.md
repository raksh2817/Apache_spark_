# 🚀 Apache Spark Exploration Project

**Professor:** *Tyler J Conlon*  
**Semester:** Spring 2025  
**Student:** Rakshith Srinath

## 📋 Table of Contents
1. [Project Overview](#-project-overview)
2. [Project Objectives](#-what-im-aiming-for)
3. [Introduction to Apache Spark](#-a-quick-introduction-to-apache-spark)
4. [Background & Rationale](#-background-why-spark-matters)
5. [Spark Architecture](#-apache-spark-architecture--breaking-it-down)
6. [Comparative Analysis](#-spark-vs-the-rest--how-it-stacks-up)
7. [Installation Guide](#-installing-apache-spark-on-windows)
   - [Java JDK Installation](#-step-1-install-java-jdk)
   - [Python Installation](#-step-2-install-python)
   - [Apache Spark Setup](#-step-3-download-apache-spark)
   - [Hadoop Configuration](#-step-4-download-hadoop-winutils)
   - [Environment Variables](#-step-5-configure-environment-variables)
   - [Creating a SparkSession](#-step-5-starting-a-custom-sparksession-in-python)
8. [Data Description](#-data-description)
   - [Flight Telemetry Data](#-flight-telemetry-data)
   - [Aircraft Model Reference Data](#-aircraft-model-reference-data)
9. [Basic Spark Queries](#-Basic-Spark-Queries)

---

## 📘 Project Overview

Welcome to my hands-on journey into the world of **Apache Spark**! This project is a deep dive into the inner workings of Spark, aimed at building practical experience, strengthening my understanding of distributed computing, and showing how Spark makes big data manageable.

---

## 🎯 What I'm Aiming For

- 🔍 Explore Apache Spark's architecture and components.
- ⚙️ Understand how Spark handles large-scale data efficiently.
- 🧪 Build and document use-case-driven examples that showcase Spark's power.

---

## 🔥 A Quick Introduction to Apache Spark

Apache Spark is an open-source, lightning-fast, distributed data processing framework. Its key strengths? Speed, simplicity, and scalability. From data wrangling to real-time analytics and machine learning — Spark covers it all!

In this project, I'll be exploring Spark's ecosystem and architecture through guided experiments, sharing insights and challenges along the way.

---

## 🧠 Background: Why Spark Matters

Originally created in 2009 by researchers at UC Berkeley, Apache Spark evolved as a faster, more flexible alternative to Hadoop's MapReduce. Its in-memory data processing makes it ideal for iterative computations like machine learning and graph processing.

### 🧾 Why I Chose Apache Spark

- 🚀 **Speed:** Spark can run tasks up to 100x faster than MapReduce thanks to in-memory computing.
- 🧰 **Developer-Friendly:** APIs in Python, Scala, Java, and R.
- 🌐 **Versatility:** Comes with built-in libraries for SQL (Spark SQL), Machine Learning (MLlib), Graph Processing (GraphX), and Streaming (Structured Streaming).

---

## 🧱 Apache Spark Architecture — Breaking It Down

Spark's architecture is elegant yet powerful. It's designed to manage distributed data across multiple nodes in a cluster seamlessly.

![Apache Spark Architecture](spark_architecture.png)

### 🧠 1. Driver Program (aka SparkContext)
- Acts as the brain of the operation.
- Initiates the Spark job, coordinates tasks, and collects results.

### 📦 2. Cluster Manager
- Think of this as Spark's resource scheduler.
- Assigns worker nodes (executors) based on available memory and CPU.
- Examples: **Standalone**, **YARN**, **Mesos**, **Kubernetes**.

### 🛠️ 3. Executors (on Worker Nodes)
- Do the heavy lifting — executing tasks, caching data, and reporting back to the Driver.

### 🔄 End-to-End Workflow
1. Driver requests resources from Cluster Manager.
2. Cluster Manager assigns Executors.
3. Driver divides jobs into tasks and hands them off.
4. Executors process tasks and return the results.

---

## 🤔 Spark vs. The Rest — How It Stacks Up


| Feature            | Apache Spark                           | Hadoop MapReduce           |          
|--------------------|----------------------------------------|----------------------------|
| **Speed**          | In-memory processing, very fast        | Disk-based, slower         | 
| **Ease of Use**    | Simple APIs in multiple languages      | Java-heavy, complex APIs   | 
| **Processing Type**| Batch & real-time supported            | Batch only                 | 
| **Ecosystem**      | SQL, MLlib, Streaming, GraphX          | Needs extra components     | 
| **Fault Tolerance**| Lineage-based recovery                 | Replication-based          | 

> **✅ Verdict:** Spark is a flexible, high-performance solution ideal for modern data workflows.

---

## 🛠️ Installing Apache Spark on Windows

Here's a simple, step-by-step setup guide for getting Spark up and running on Windows:

### ✅ Step 1: Install Java JDK

1. Download JDK from [Adoptium](https://adoptium.net/)
2. Move it to: `C:\Java\jdk`
3. To verify, open **Command Prompt** and run:

```bash
java -version
```

### ✅ Step 2: Install Python

1. Grab Python (3.x) from [python.org](https://www.python.org/downloads/)
2. During installation, **check** the box that says: `Add Python to PATH`
3. To verify, open Command Prompt and run:

```bash
python --version
```

### ✅ Step 3: Download Apache Spark

1. Visit [Apache Spark Downloads](https://spark.apache.org/downloads.html)
2. Select version: `Spark 3.5.1 (latest stable)`
3. Choose: `Pre-built for Apache Hadoop 3.3 or later`
4. Extract and move folder to: `C:\Spark`

### ✅ Step 4: Download Hadoop (winutils.exe)

1. Download from [Hadoop Winutils GitHub](https://github.com/steveloughran/winutils)
2. Extract and move to: `C:\Hadoop\bin`

### ✅ Step 5: Configure Environment Variables

1. Right-click **"This PC"** → **"Properties"**
2. Click **"Advanced system settings"** → **"Environment Variables"**
3. Add the following **System Variables**:

| Variable Name | Value                                |
|---------------|--------------------------------------|
| JAVA_HOME     | `C:\Java\jdk-<your-version>`         |
| SPARK_HOME    | `C:\Spark`                           |
| HADOOP_HOME   | `C:\Hadoop\bin`                      |

> ✨ **Pro Tip:** Replace `<your-version>` with your actual JDK folder name (e.g., `jdk-17`).

That's it! You're ready to run your first Apache Spark application. 💻

### ✅ Step 6: Starting a Custom SparkSession in Python

You can manually start Apache Spark within a Python script using the `pyspark` library.

This gives you full control over Spark configurations and is useful when writing data processing scripts locally.

#### Sample Python Script

Create a new `.py` file (for example: `start_spark.py`) and add the following code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyLocalSparkApp") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
```

---

## ✈️ Data Description

This project uses real-world flight data sourced from an **ADS-B server** — a system that tracks aircraft via transponder signals. While the live feed updates daily, we're working with **static snapshot extracts** so that our analysis remains consistent, repeatable, and benchmarkable.

### 🛰️ Flight Telemetry Data

Think of this as a live logbook for each aircraft in the sky. Each snapshot captures multiple records of aircraft telemetry, with detailed flight data wrapped in a JSON format.

**🔧 Format:** JSON  
**📦 Structure:** Each entry includes a `timestamp` and a `payload` — a nested object holding detailed flight metrics.

#### 🗝️ Key Fields Explained:

- **`dt`** – Timestamp of when the data was captured.  
- **`payload.hex`** – Aircraft's unique 24-bit identifier (like a digital license plate).  
- **`payload.alt_baro`** – Altitude measured using barometric pressure (in feet).  
- **`payload.ias`** – Indicated Airspeed (what pilots see in the cockpit).  
- **`payload.mach`** – Mach number, i.e., speed relative to the speed of sound.  
- **`payload.mag_heading`** – Magnetic compass heading.  
- **`payload.baro_rate` / `payload.geom_rate`** – Vertical speed rates via barometric and geometric sources.  
- **`payload.nav_qnh`** – Atmospheric pressure setting used for altitude calibration.  
- **`payload.nav_altitude_mcp` / `nav_altitude_fms`** – Navigation altitudes from different flight systems.  
- **`payload.nav_modes`** – List of active autopilot/navigation modes.  
- **`payload.mlat` / `payload.tisb`** – Supplemental data arrays (may be empty).  
- **`payload.messages`** – Total number of messages received from the aircraft.  
- **`payload.seen`** – Duration (in seconds) the aircraft was visible to the server.  
- **`payload.rssi`** – Signal strength of the received messages.

📌 *Note:* For easier analysis, these nested fields are **flattened** into top-level columns when loaded into Spark.

### 🛩️ Aircraft Model Reference Data

This dataset complements the telemetry data by providing **aircraft-specific details** like manufacturer, model, and build year. It's used as a **lookup table** to enrich telemetry records, linked via the common aircraft identifier (`hex` / `icao`). 

#### 🔑 Key Columns:

- **`icao`** – Aircraft's unique ICAO address (same as `payload.hex`).  
- **`reg`** – Aircraft registration number (like its tail number).  
- **`icatype`** – Aircraft type/class code.  
- **`year`** – Manufacturing year (if available).  
- **`manufacturer`** – Brand name (e.g., Airbus, Boeing).  
- **`model`** – Specific aircraft model or variant.  
- **`ownop`** – Ownership or operator info.  
- **`faa_ladd`** – FAA privacy flag (true/false).  
- **`short_type`** – Shortened aircraft classification.  
- **`mil`** – Is it military? (true/false)

---

These two datasets — one for **real-time telemetry** and one for **aircraft metadata** — work together to provide a complete picture of flight operations. While the original data is refreshed daily, we're working with selected **static snapshots** to perform controlled, scalable analysis at different data volumes.



---

## 🧮 Basic Spark Queries

Once Spark is configured, running basic queries is a great way to explore its capabilities. Using the `DataFrame API` or `Spark SQL`, we can perform typical data operations efficiently.

Here’s a glimpse into commonly used queries:

```python
# Load full telemetry and extract all payload fields in Spark
df_spark_small = spark.read.json(small_telemetry_path)

# Show schema
df_flight.printSchema()

# to Flatten 
from pyspark.sql.functions import col

df_spark_small_flat = df_flight.select(
    col("dt").alias("timestamp"),
    col("payload.hex").alias("hex"),
    col("payload.alt_baro").alias("alt_baro"),
    col("payload.alt_geom").alias("alt_geom"),
    col("payload.gs").alias("ground_speed"),
    col("payload.track").alias("track"),
    col("payload.baro_rate").alias("baro_rate"),
    col("payload.squawk").alias("squawk"),
    col("payload.emergency").alias("emergency"),
    col("payload.category").alias("category"),
    col("payload.nav_qnh").alias("nav_qnh"),
    col("payload.nav_altitude_mcp").alias("nav_altitude_mcp"),
    col("payload.nav_heading").alias("nav_heading"),
    col("payload.lat").alias("lat"),
    col("payload.lon").alias("lon"),
    col("payload.nic").alias("nic"),
    col("payload.rc").alias("rc"),
    col("payload.seen_pos").alias("seen_pos"),
    col("payload.version").alias("version"),
    col("payload.nic_baro").alias("nic_baro"),
    col("payload.nac_p").alias("nac_p"),
    col("payload.nac_v").alias("nac_v"),
    col("payload.sil").alias("sil"),
    col("payload.sil_type").alias("sil_type"),
    col("payload.gva").alias("gva"),
    col("payload.sda").alias("sda"),
    col("payload.mlat").alias("mlat"),
    col("payload.tisb").alias("tisb"),
    col("payload.messages").alias("messages"),
    col("payload.seen").alias("seen"),
    col("payload.rssi").alias("rssi"),
    col("payload.flight").alias("flight")
)


# Show first 5 rows
df_spark_small_flat.show(5, truncate=False)

# Count total number of records
df_spark_small_flat.count()

# to select records 
df_spark_small_flat.select("hex", "alt_baro", "ground_speed", "track", "lat", "lon") \
                   .show(5, truncate=False)

# to filter records 

df_spark_small_flat.filter(col("alt_baro") > 30000) \
                   .select("hex", "alt_baro", "ground_speed") \
                   .show(5)

# to count records per group

df_spark_small_flat.groupBy("hex") \
                   .count() \
                   .orderBy("count", ascending=False) \
                   .show(5)

# to find out avg per group 

df_spark_small_flat.groupBy("hex") \
                   .agg(avg("ground_speed").alias("avg_speed")) \
                   .orderBy("avg_speed", ascending=False) \
                   .show(5)

# to add new column 

df_spark_small_flat.withColumn("speed_knots", round(col("ground_speed") * 0.539957, 2)) \
                   .select("hex", "ground_speed", "speed_knots") \
                   .show(5)

# to join 2 datasets 

df_joined = df_spark_small_flat.join(
    df_aircraft,
    df_spark_small_flat.hex == df_aircraft.icao,
    how="left"
)

df_joined.select("hex", "alt_baro", "manufacturer", "model", "reg").show(5)

# using spark sql 

df_spark_small_flat.createOrReplaceTempView("flights")

#to select flights flying above 30,000 ft
 
spark.sql("""
    SELECT hex, alt_baro, ground_speed
    FROM flights
    WHERE alt_baro > 30000
    LIMIT 5
""").show()

# count total flights per aircraft 

spark.sql("""
    SELECT hex, COUNT(*) as flight_count
    FROM flights
    GROUP BY hex
    ORDER BY flight_count DESC
    LIMIT 5
""").show()
