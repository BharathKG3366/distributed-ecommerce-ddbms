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

```

---

## 🛠️ Distributed Technologies & Roles

| Tool / Technology | System Role | Detailed Configuration |
| --- | --- | --- |
| **MongoDB 7.0 Community** | Distributed NoSQL Storage | Configured as Replica Set `rs0` with 3 nodes (`27021`, `27022`, `27023`). Uses WiredTiger engine and `oplog.rs` synchronization. |
| **Apache ZooKeeper 3.9** | Cluster Coordination | Runs on port `2181`. Persists active cluster member configuration and health state at znode `/ecommerce_ddbms/cluster_config`. |
| **Apache Spark Engine** | Parallel Rule Mining | Computes offline cross-category collaborative association rules stored in `spark_copurchase_rules`. |
| **Python FastAPI & Uvicorn** | Distributed Routing Middleware | High-throughput async REST server. Implements dynamic read preference routing, session carts, and live `.explain()` profiling. |
| **Docker & Docker Compose** | Container Orchestration | Isolates all four backend services in a unified network namespace with persistent storage volumes. |
| **Tailwind CSS & Chart.js** | Client Storefront & Telemetry | Interactive e-commerce catalog featuring Light/Dark themes, slide-out drawer, and dynamic query execution bar charts. |

---

## 🗄️ Database Schema & Collections

The platform operates on the `ecommerce_ddbms` database with 5 collections:

1. **`products`** (32 Documents): Catalog spanning 8 categories (`Mobiles`, `Laptops`, `Audio`, `Fashion & Ethnic`, `Home & Kitchen`, `Smart Wearables`, `Gaming & Accessories`, `Beauty & Care`). Compound index: `idx_category_rating` (`{ category: 1, rating: -1 }`).
2. **`user_interactions`** (5,980 Documents): User behavioral logs with weighted events (`view: +1`, `wishlist: +3`, `cart: +5`). Compound index: `idx_user_category` (`{ user_id: 1, category: 1 }`).
3. **`spark_copurchase_rules`** (8 Documents): Collaborative filtering association rules (e.g., `Audio -> Wearables` with 89% confidence).
4. **`user_carts`** (Dynamic Documents): Persists active cart quantities across browser sessions using atomic `$inc` updates.
5. **`telemetry_logs`** (Audit Trail): Stores execution time, query plan, examined document counts, and routed node for every request.

---

## 🚀 How to Run (Execution Guide)

### Option A: Running Locally with Docker Desktop (Recommended)

#### 1. Prerequisites

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
* [Python 3.10+](https://www.python.org/downloads/) installed.
* Git installed.

#### 2. Clone the Repository

```bash
git clone [https://github.com/](https://github.com/)<YOUR-GITHUB-USERNAME>/distributed-ecommerce-ddbms.git
cd distributed-ecommerce-ddbms

```

#### 3. Install Python Dependencies

```bash
pip install fastapi uvicorn pymongo kazoo pydantic

```

#### 4. Launch Distributed Containers

Start the three MongoDB replica nodes and Apache ZooKeeper in background mode:

```bash
docker compose up -d

```

*(Verify that `mongo1`, `mongo2`, `mongo3`, and `ddbms-zookeeper` show a green "Running" status in Docker Desktop).*

#### 5. Initialize Replica Set `rs0` & Seed Data

Execute the cluster setup script to configure replica voting, apply SCRAM-SHA-256 security, register with ZooKeeper, seed the 32 products, and insert 5,980 interaction logs:

```bash
python seed_and_spark.py

```

#### 6. Start the Application Middleware

```bash
python -m uvicorn main:app --reload

```

Open your browser and navigate to:

```text
http://localhost:8000

```

---

### Option B: Running in Cloud via GitHub Codespaces

Evaluators can run the full multi-node cluster directly in their browser without installing Docker or Python locally:

1. Click the green **`<> Code`** button at the top-right of this repository.
2. Select the **Codespaces** tab and click **"Create codespace on main"**.
3. Once the cloud terminal opens at the bottom, execute:
```bash
pip install fastapi uvicorn pymongo kazoo pydantic
docker compose up -d
python seed_and_spark.py
python -m uvicorn main:app --host 0.0.0.0 --port 8000

```


4. Click **"Open in Browser"** when the port 8000 forwarded notification appears.

---

## 🧪 Step-by-Step Demonstration for Evaluators

1. **Demonstrating Distributed Query Optimization (CO3 / Lab 8):**
* Click **`COLLSCAN (Unindexed)`** in the top navigation bar:
* MongoDB performs a full collection scan across all **5,980 documents** with **0 index keys examined**, taking **4 ms** in the engine.


* Click **`IXSCAN (Indexed)`** in the top navigation bar:
* The engine leverages the compound B-Tree index `idx_user_category` (`{ user_id: 1, category: 1 }`), pruning the scan to **5 documents** and **5 index keys** in **1 ms** (**99.91% reduction in scanned documents**).




2. **Demonstrating Tunable Consistency (CO5 / BASE vs. ACID):**
* Click **`Consistency: BASE (Secondary)`**:
* Read queries are offloaded to **`Secondary Replica (27022 / 27023)`** using `ReadPreference.SECONDARY_PREFERRED` with asynchronous write acknowledgement (`w=1`).


* Click **`Consistency: ACID (Primary)`**:
* Read queries are routed strictly to the **`Primary Coordinator (27021)`** using `ReadPreference.PRIMARY` with majority write durability (`w="majority", j=True`).




3. **Demonstrating Persistent Cart & Spark Recommendations:**
* Active profile: **Ananya Sharma (Bengaluru)** with 4 items in cart (`3x Sony WH-1000XM5`, `1x Dyson V12 Vacuum`).
* Interest profile evaluates to `[ Audio: 15 pts • Home & Kitchen: 5 pts • Mobiles: 5 pts ]`.
* The first 3 recommendation slots display top Audio items (*Sony*, *Marshall*, *AirPods Pro*).
* The 4th slot displays the **Apache Spark Co-Purchase Rule** (`Audio -> Wearables` at 89% confidence), recommending the *Garmin Forerunner 965* / *Apple Watch Ultra 2*.
* Refresh the page to verify that cart items and telemetry history survive browser restarts via MongoDB persistent collections.



---

## 📊 Empirical Performance Benchmarks

The following table contains live execution metrics captured in the `telemetry_logs` collection:

| Timestamp | Customer ID | Consistency Model | Winning Execution Plan | Docs Scanned (`totalDocsExamined`) | Keys Examined (`totalKeysExamined`) | Routed Replica Node | DB Engine Time | Total Roundtrip Latency |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `14:28:46` | `ananya_sharma` | `BASE` | `COLLSCAN (Full Scan)` | 5,980 | 0 | Secondary (`27022 / 27023`) | 4 ms | 23.00 ms |
| `14:28:47` | `ananya_sharma` | `BASE` | `COLLSCAN (Full Scan)` | 5,980 | 0 | Secondary (`27022 / 27023`) | 4 ms | 68.84 ms |
| `14:28:51` | `ananya_sharma` | `BASE` | `COLLSCAN (Full Scan)` | 5,980 | 0 | Secondary (`27022 / 27023`) | 4 ms | 46.80 ms |
| `14:29:03` | `ananya_sharma` | `BASE` | `COLLSCAN (Full Scan)` | 5,980 | 0 | Secondary (`27022 / 27023`) | 4 ms | 37.69 ms |
| `14:29:32` | `ananya_sharma` | **`ACID`** | **`IXSCAN (Compound Index)`** | **5** | **5** | **Primary Coordinator (`27021`)** | **1 ms** | **31.46 ms** |
| `14:29:33` | `ananya_sharma` | **`BASE`** | **`IXSCAN (Compound Index)`** | **5** | **5** | **Secondary Replica (`27022 / 27023`)** | **1 ms** | **32.08 ms** |
| `14:29:35` | `ananya_sharma` | `BASE` | `COLLSCAN (Full Scan)` | 5,980 | 0 | Secondary (`27022 / 27023`) | 4 ms | 36.23 ms |

### Summary of Performance Improvements

* **Document Scan Volume:** Reduced from 5,980 to 5 documents (**99.916% reduction**).
* **Internal Database Latency:** Reduced from 4 ms to 1 ms (**75.0% speedup**).
* **Primary Node Offloading:** In `BASE` mode, 100% of analytical read queries are handled by secondary replicas, leaving the primary coordinator available for write transactions.

---

## 📂 Repository File Structure

```text
├── docker-compose.yml       # Docker definition for 3-node MongoDB Replica Set & Apache ZooKeeper
├── seed_and_spark.py        # Replica set initializer, RBAC setup, data seeder & Spark rule loader
├── main.py                  # FastAPI server with consistency routing, cart APIs & .explain() profiler
├── index.html               # Responsive frontend UI, cart drawer, telemetry panel & Chart.js graph
├── .gitignore               # Excludes bytecode (__pycache__), virtualenvs, and data volumes
└── README.md                # Project documentation and execution instructions

```

---

## 🎓 Course Syllabus & Rubric Mapping

This project maps directly to the following laboratory exercises and course outcomes:

* **(Query Optimization using MongoDB Compass):** Evaluated via compound indexing (`idx_user_category`, `idx_category_rating`) and empirical `.explain("executionStats")` metrics.
* **(Parallel Query Processing using Spark):** Evaluated via offline collaborative filtering rules loaded into `spark_copurchase_rules`.
* **(Distributed Coordination with Apache ZooKeeper):** Evaluated via automated cluster registration and health checks at `/ecommerce_ddbms/cluster_config`.
* **(Distributed Database Security & Authentication):** Evaluated via SCRAM-SHA-256 user authentication and least-privilege RBAC provisioning.
* **Course Outcomes:** **CO3** (Query Optimization - RBT4) and **CO5** (Distributed Storage & Security - RBT4/RBT5).

```

```
