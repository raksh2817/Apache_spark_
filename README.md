# 🚀 Apache Spark Exploration Project

**Professor:** *Tyler J Conlon*  
**Semester:** Spring 2025  
**Student:** Rakshith Srinath

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

In this project, I’ll be exploring Spark’s ecosystem and architecture through guided experiments, sharing insights and challenges along the way.

---

## 🧠 Background: Why Spark Matters

Originally created in 2009 by researchers at UC Berkeley, Apache Spark evolved as a faster, more flexible alternative to Hadoop’s MapReduce. Its in-memory data processing makes it ideal for iterative computations like machine learning and graph processing.

### 🧾 Why I Chose Apache Spark

- 🚀 **Speed:** Spark can run tasks up to 100x faster than MapReduce thanks to in-memory computing.
- 🧰 **Developer-Friendly:** APIs in Python, Scala, Java, and R.
- 🌐 **Versatility:** Comes with built-in libraries for SQL (Spark SQL), Machine Learning (MLlib), Graph Processing (GraphX), and Streaming (Structured Streaming).

---

## 🧱 Apache Spark Architecture — Breaking It Down

Spark's architecture is elegant yet powerful. It’s designed to manage distributed data across multiple nodes in a cluster seamlessly.

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


| Feature            | Apache Spark                           | Hadoop MapReduce                     | Apache Flink                    |
|--------------------|----------------------------------------|--------------------------------------|----------------------------------|
| **Speed**          | In-memory processing, very fast        | Disk-based, slower                   | Also fast, designed for streaming |
| **Ease of Use**    | Simple APIs in multiple languages       | Java-heavy, complex APIs             | Java/Scala focused               |
| **Processing Type**| Batch & real-time supported             | Batch only                           | Real-time optimized              |
| **Ecosystem**      | SQL, MLlib, Streaming, GraphX           | Needs extra components               | Streaming-first design           |
| **Fault Tolerance**| Lineage-based recovery                  | Replication-based                    | Checkpointing                    |

---

✅ Verdict: Spark is a flexible, high-performance solution ideal for modern data workflows.

---

# 🛠️ Installing Apache Spark on Windows

Here’s a simple, step-by-step setup guide for getting Spark up and running on Windows:

---

## ✅ Step 1: Install Java JDK

- Download JDK from [Adoptium](https://adoptium.net/)
- Move it to: `C:\Java\jdk`
- To verify, open **Command Prompt** and run:

```bash
java -version
```

---

## ✅ Step 2: Install Python

- Grab Python (3.x) from [python.org](https://www.python.org/downloads/)
- During installation, **check** the box that says: `Add Python to PATH`
- To verify, open Command Prompt and run:

```bash
python --version
```

---

## ✅ Step 3: Download Apache Spark

- Visit [Apache Spark Downloads](https://spark.apache.org/downloads.html)
- Select version: `Spark 3.5.1 (latest stable)`
- Choose: `Pre-built for Apache Hadoop 3.3 or later`
- Extract and move folder to: `C:\Spark`

---

## ✅ Step 4: Download Hadoop (winutils.exe)

- Download from [Hadoop Winutils GitHub](https://github.com/steveloughran/winutils)
- Extract and move to: `C:\Hadoop\bin`

---

## ✅ Step 5: Configure Environment Variables

1. Right-click **"This PC"** → **"Properties"**
2. Click **"Advanced system settings"** → **"Environment Variables"**
3. Add the following **System Variables**:

| Variable Name | Value                                |
|---------------|----------------------------------------|
| JAVA_HOME     | `C:\Java\jdk-<your-version>`         |
| SPARK_HOME    | `C:\Spark`                           |
| HADOOP_HOME   | `C:\Hadoop\bin`                     |

> ✨ Replace `<your-version>` with your actual JDK folder name (e.g., `jdk-17`).

That’s it! You’re ready to run your first Apache Spark application. 💻

---

## 🛠️✅ Step 5: Starting a Custom SparkSession in Python

You can manually start Apache Spark within a Python script using the `pyspark` library.

This gives you full control over Spark configurations and is useful when writing data processing scripts locally.

---

### ✅ Sample Python Script

Create a new `.py` file (for example: `start_spark.py`) and add the following code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyLocalSparkApp") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

---

## ✈️ Data Description

This project uses real-world flight data sourced from an **ADS-B server** — a system that tracks aircraft via transponder signals. While the live feed updates daily, we’re working with **static snapshot extracts** so that our analysis remains consistent, repeatable, and benchmarkable.

---

### 🛰️ Flight Telemetry Data

Think of this as a live logbook for each aircraft in the sky. Each snapshot captures multiple records of aircraft telemetry, with detailed flight data wrapped in a JSON format.

**🔧 Format:** JSON  
**📦 Structure:** Each entry includes a `timestamp` and a `payload` — a nested object holding detailed flight metrics.

#### 🗝️ Key Fields Explained:

- **`dt`** – Timestamp of when the data was captured.  
- **`payload.hex`** – Aircraft’s unique 24-bit identifier (like a digital license plate).  
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

---

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

These two datasets — one for **real-time telemetry** and one for **aircraft metadata** — work together to provide a complete picture of flight operations. While the original data is refreshed daily, we’re working with selected **static snapshots** to perform controlled, scalable analysis at different data volumes.


