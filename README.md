# Apache Spark Exploration Project

  
**Professor:** *Tyler J Conlon*  
**Semester:** Spring 2025    
**Student:** Rakshith Srinath



## 📖 Overview

This project involves exploring **Apache Spark** and documenting the learning journey as part of integrating Spark into the course curriculum. The main goal is gaining practical experience in Spark's architecture and data-processing capabilities.



## 🎯 Objectives

- Gain practical knowledge of Apache Spark.
- Understand Spark’s architecture and features.
- Develop and document examples demonstrating Spark’s strengths.



## 🚀 Introduction

**Apache Spark** is an open-source, distributed computing framework designed for processing large-scale data quickly and efficiently. It is particularly renowned for its speed, ease of use, and powerful data processing capabilities.

This project explores the features and functionality of Apache Spark, showcasing its advantages, practical uses, and limitations through hands-on experiments and analyses.  



## 📚 Background

Apache Spark was originally developed in 2009 by researchers at the University of California, Berkeley, as an improvement over Hadoop MapReduce. Spark addresses MapReduce’s limitations—particularly its speed and ease of iterative tasks—by processing data in-memory rather than storing intermediate results to disk.

### Why Apache Spark?
- **Speed:** Up to 100x faster than traditional Hadoop MapReduce for certain tasks, thanks to its in-memory computation.
- **Ease of Use:** Offers simple APIs in Python, Scala, Java, and R.
- **Rich Ecosystem:** Includes libraries like Spark SQL for structured data processing, MLlib for machine learning, GraphX for graph processing, and Structured Streaming for real-time data processing.



## 🚀 Apache Spark Architecture

Apache Spark follows a straightforward, distributed architecture designed to handle large-scale data processing quickly and efficiently. Its architecture mainly consists of three key components:

![Apache Spark Architecture](spark_architecture.png)

### 1. **Driver Program (SparkContext)**

- **Role:** Central coordinator and controller of a Spark application.
- **Responsibilities:**
  - Establishing and maintaining a connection to the cluster.
  - Distributing tasks to executors.
  - Aggregating results from executors.

### 2. **Cluster Manager**

- **Role:** Handles resource management across the cluster.
- **Responsibilities:**
  - Allocating resources (memory, CPU) requested by the Driver.
  - Assigning Executors to Worker Nodes.
- **Common Cluster Managers:** Standalone, Apache YARN, Kubernetes, Apache Mesos.

### 3. **Worker Nodes (Executors)**

- **Role:** Perform actual computation and data processing tasks.
- **Responsibilities:**
  - Executing tasks assigned by the Driver Program.
  - Managing data caching to optimize performance.
  - Returning results to the Driver upon completion.

### 🔄 **Workflow Summary:**

- **Step 1:** Driver Program requests resources from the Cluster Manager.
- **Step 2:** Cluster Manager allocates executors on available Worker Nodes.
- **Step 3:** Driver divides work into tasks and distributes them to Executors.
- **Step 4:** Executors perform computations and return results to the Driver Program.


## 🔍 How Apache Spark is Different from Other Frameworks

Apache Spark differentiates itself significantly from traditional big data frameworks like **Hadoop MapReduce**, **Apache Flink**, and standard relational databases. The key differences include:

### 1. **In-Memory Computation**
- **Spark:** Primarily performs computations in memory, significantly speeding up iterative tasks and machine learning processes.
- **Hadoop MapReduce:** Writes intermediate data to disk between each step, resulting in slower processing.

### 2. **Unified Analytics Engine**
- **Spark:** Offers integrated modules (Spark SQL, MLlib, Structured Streaming, GraphX) that provide a comprehensive platform for diverse analytical workloads.
- **Other Frameworks:** Often require separate tools or platforms for SQL processing, machine learning, streaming, and graph processing.

### 3. **Ease of Use and Flexibility**
- **Spark:** Provides easy-to-use APIs in multiple languages (Python, Scala, Java, R), making development accessible and rapid.
- **Hadoop & Traditional Tools:** Typically have more complex APIs or restricted language options, increasing learning curves.

### 4. **Real-Time and Batch Processing**
- **Spark:** Seamlessly supports both batch and real-time data processing within the same framework.
- **Apache Flink:** Optimized mainly for streaming and real-time analytics, less intuitive for batch processing.
- **Hadoop MapReduce:** Primarily optimized for batch processing tasks only.

### 5. **Fault Tolerance and Recovery**
- **Spark:** Uses lineage information (RDD transformations) for fault tolerance, allowing rapid and automatic recovery without manual intervention.
- **Other Frameworks (e.g., Hadoop):** Typically rely on replicating data across multiple nodes, requiring more disk space and slower recovery times.

In summary, Apache Spark’s combination of speed, ease of use, unified analytics capabilities, and flexible processing options clearly distinguishes it from traditional big data frameworks.


# 🔧 Apache Spark Installation (Windows)

Follow each step carefully to install Apache Spark successfully.



## ✅ Step 1: Install Java JDK

- Download JDK from [Adoptium](https://adoptium.net/).
- Install Java and confirm by running:
- After installation, verify by opening Command Prompt and typing:

- Move the downloaded folder to: 'C:\Java\jdk'

```bash
java -version

## ✅ Step 2: Install Python

- Download Python (3.x) from the official website:
  [https://www.python.org/downloads/](https://www.python.org/downloads/)

- During installation, select:
  - ✅ **"Add Python to PATH"**

- After installation, verify by opening Command Prompt and typing:
```bash
python --version

## ✅ Step 3: Download Apache Spark

- Go to the official [Apache Spark Downloads page](https://spark.apache.org/downloads.html).

- Under **Spark Release**, select version:
  - ✅ **Spark 3.5.1 (latest stable version)**

- For **Package Type**, choose:
  - ✅ **"Pre-built for Apache Hadoop 3.3 or later"**

- Click the download link and save the file.

- Extract the downloaded file using any Extractor. 

- Move and rename the extracted folder to: 'C:\Spark'

## ✅ Step 4: Download Hadoop (`winutils.exe`)

- Apache Spark requires Hadoop binaries for Windows compatibility.

- Download Hadoop binaries (`winutils.exe`) from:
  [Hadoop Windows binaries](https://github.com/steveloughran/winutils).

- Extract the downloaded archive.

- Move the extracted Hadoop folder to: 'C:\Hadoop\bin'

## ✅ Step 5: Configure Environment Variables

Set the environment variables required by Apache Spark, Hadoop, and Java:

---

### 🔹 Open Environment Variables

- Right-click **"This PC"** → **"Properties"**
- Click **"Advanced system settings"** → **"Environment Variables"**

---

### 🔹 Add the following **System Variables**:

| Variable Name | Variable Value            |
|---------------|---------------------------|
| `JAVA_HOME`   | `C:\Java\jdk-<your-version>` *(e.g., jdk-17)* |
| `SPARK_HOME`  | `C:\Spark`                |
| `HADOOP_HOME` | `C:\Hadoop\bin`               |

> 🔸 Replace `<your-version>` with the actual folder name of your installed JDK.

---

