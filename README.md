# Apache Spark Exploration Project

  
**Professor:** *Tyler J Conlon*  
**Semester:** Spring 2025    
**Student:** Rakshith Srinath

---

## 📖 Overview

This project involves exploring **Apache Spark** and documenting the learning journey as part of integrating Spark into the course curriculum. The main goal is gaining practical experience in Spark's architecture and data-processing capabilities.

---

## 🎯 Objectives

- Gain practical knowledge of Apache Spark.
- Understand Spark’s architecture and features.
- Develop and document examples demonstrating Spark’s strengths.

---

## 🚀 Introduction

**Apache Spark** is an open-source, distributed computing framework designed for processing large-scale data quickly and efficiently. It is particularly renowned for its speed, ease of use, and powerful data processing capabilities.

This project explores the features and functionality of Apache Spark, showcasing its advantages, practical uses, and limitations through hands-on experiments and analyses.  

---

## 📚 Background

Apache Spark was originally developed in 2009 by researchers at the University of California, Berkeley, as an improvement over Hadoop MapReduce. Spark addresses MapReduce’s limitations—particularly its speed and ease of iterative tasks—by processing data in-memory rather than storing intermediate results to disk.

### Why Apache Spark?
- **Speed:** Up to 100x faster than traditional Hadoop MapReduce for certain tasks, thanks to its in-memory computation.
- **Ease of Use:** Offers simple APIs in Python, Scala, Java, and R.
- **Rich Ecosystem:** Includes libraries like Spark SQL for structured data processing, MLlib for machine learning, GraphX for graph processing, and Structured Streaming for real-time data processing.

---

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

---


