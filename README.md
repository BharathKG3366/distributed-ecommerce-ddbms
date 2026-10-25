# ElectroMart India: Distributed E-Commerce Recommendation Data Platform

[![Course](https://img.shields.io/badge/Course-Distributed_Database_Systems_-blue.svg)]()
[![Degree](https://img.shields.io/badge/Degree-M.Tech_CSE_(2026--2027)-purple.svg)]()
[![Architecture](https://img.shields.io/badge/Cluster-3--Node_MongoDB_Replica_Set_(rs0)-green.svg)]()
[![Coordination](https://img.shields.io/badge/Coordination-Apache_ZooKeeper-amber.svg)]()

A containerized distributed database platform demonstrating **tunable consistency models (BASE vs. ACID)** and **empirical distributed query optimization (IXSCAN vs. COLLSCAN)** over an e-commerce catalog of 32 products and 5,980 behavioral interaction logs.

---

## 🏛️ System Architecture Overview

- **Storage Tier (MongoDB 7.0 Replica Set `rs0`):**
  - `mongo1` (Primary Coordinator | Port `27021` | Priority 3): Handles client writes and ACID majority reads.
  - `mongo2` & `mongo3` (Secondary Replicas | Ports `27022`, `27023` | Priority 1): Asynchronously replicate `oplog.rs` and serve load-balanced BASE reads.
- **Coordination Tier:** Apache ZooKeeper (`Port 2181`) maintains cluster topology at znode `/ecommerce_ddbms/cluster_config`.
- **Analytics Tier:** Apache Spark collaborative filtering association rules (`spark_copurchase_rules`) mapped across 8 retail categories.
- **Middleware Tier:** Python FastAPI asynchronous REST router with integrated `.explain("executionStats")` diagnostic telemetry profiler.
- **Presentation Tier:** Single-page storefront built with Tailwind CSS, Chart.js execution visualizer, and persistent cart drawer.

---

## 🚀 Quickstart & Reproduction Guide

### Prerequisites
- Docker Engine & Docker Desktop
- Python 3.10+
- Git

### 1. Launch Distributed Cluster
```bash
docker compose up -d
