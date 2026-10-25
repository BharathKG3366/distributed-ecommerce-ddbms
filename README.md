# ElectroMart India: Distributed E-Commerce Recommendation Data Platform

[![Course](https://img.shields.io/badge/Course-Distributed_Database_Systems_(CF013)-blue.svg)]()
[![Degree](https://img.shields.io/badge/Degree-M.Tech_CSE_(2026--2027)-purple.svg)]()
[![Cluster](https://img.shields.io/badge/Cluster-3--Node_MongoDB_Replica_Set_(rs0)-green.svg)]()
[![Coordination](https://img.shields.io/badge/Coordination-Apache_ZooKeeper-amber.svg)]()
[![Analytics](https://img.shields.io/badge/Analytics-Apache_Spark_Collaborative_Filtering-red.svg)]()

A containerized distributed database micro-project designed and implemented for **Distributed Database Systems (Course Code: CF013)**. The platform demonstrates **tunable consistency paradigms (BASE vs. ACID)**, **empirical query optimization (IXSCAN vs. COLLSCAN)**, and a **hybrid recommendation pipeline** over a multi-category catalog of 32 products and 5,980 customer interaction records.

---

## 📌 Table of Contents
1. [System Architecture Overview](#-system-architecture-overview)
2. [Distributed Technologies & Roles](#-distributed-technologies--roles)
3. [Database Schema & Collections](#-database-schema--collections)
4. [How to Run (Execution Guide)](#-how-to-run-execution-guide)
   - [Option A: Running Locally with Docker Desktop](#option-a-running-locally-with-docker-desktop-recommended)
   - [Option B: Running in GitHub Codespaces](#option-b-running-in-cloud-via-github-codespaces)
5. [Step-by-Step Demonstration for Evaluators](#-step-by-step-demonstration-for-evaluators)
6. [Empirical Performance Benchmarks](#-empirical-performance-benchmarks)
7. [Repository File Structure](#-repository-file-structure)
8. [Course Syllabus & Rubric Mapping](#-course-syllabus--rubric-mapping)

---

## 🏛️ System Architecture Overview

The system implements a decoupled **Three-Tier Distributed Architecture** separating the presentation layer, the stateless FastAPI asynchronous router, and a stateful four-container distributed backend (3 MongoDB Replica nodes + 1 Apache ZooKeeper coordinator).

```text
+-----------------------------------------------------------------------------------------+
|                              TIER 1: PRESENTATION LAYER                                 |
|                   ElectroMart India E-Commerce Storefront (HTML5 / Tailwind)            |
|  - 8 Categories (32 Products)  |  Multi-Attribute Filters  |  Persistent Cart Drawer    |
|  - Real-Time DDBMS Telemetry Console & Chart.js Execution Plan Visualizer               |
+--------------------------------------------+--------------------------------------------+
                                             | 
                                             | HTTP REST JSON API (Port 8000)
                                             v
+-----------------------------------------------------------------------------------------+
|                              TIER 2: APPLICATION MIDDLEWARE                             |
|                           Python FastAPI Distributed Router                             |
|  - Consistency Controller: BASE Mode (w=1, Secondary Read) vs ACID Mode (w=majority)    |
|  - Query Profiler: Executes native MongoDB .explain("executionStats") on every request  |
|  - Hybrid Recommender Engine: Merges Real-Time Aggregation + Spark Co-Purchase Rules    |
+-------------+------------------------------+------------------------------+-------------+
              |                              |                              |
              | ZooKeeper Coordination       | Write & ACID Read            | BASE Read (Offloaded)
              | (Port 2181)                  | (Port 27021)                 | (Ports 27022 / 27023)
              v                              v                              v
+--------------------------+   +---------------------------+   +--------------------------+
|    APACHE ZOOKEEPER      |   |   MONGODB PRIMARY NODE    |   | MONGODB SECONDARY NODES  |
|  Container: zookeeper    |   |     Container: mongo1     |   |   Containers: mongo2/3   |
|  - Maintains cluster     |   |  - Host: 127.0.0.1:27021  |   |  - Hosts: 27022, 27023   |
|    topology znode        |   |  - Priority: 3 (Leader)   |   |  - Priority: 1           |
|  - Tracks node health    |   |  - Accepts all Client     |==>|  - Asynchronous Oplog    |
|    metadata              |   |    Writes & ACID Reads    |   |    Replication Sync      |
+--------------------------+   +---------------------------+   +--------------------------+
