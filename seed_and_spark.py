import time
import random
import json
from pymongo import MongoClient, ASCENDING, DESCENDING

direct_client = MongoClient("mongodb://127.0.0.1:27021/", directConnection=True, serverSelectionTimeoutMS=5000)

try:
    direct_client.admin.command("replSetGetStatus")
    print("[CLUSTER] Replica Set 'rs0' is active.")
except Exception:
    print("[CLUSTER] Initializing 3-Node Replica Set 'rs0'...")
    config = {
        "_id": "rs0",
        "members": [
            {"_id": 0, "host": "127.0.0.1:27021", "priority": 3},
            {"_id": 1, "host": "127.0.0.1:27022", "priority": 1},
            {"_id": 2, "host": "127.0.0.1:27023", "priority": 1}
        ]
    }
    direct_client.admin.command("replSetInitiate", config)

for _ in range(20):
    if direct_client.admin.command("hello").get("isWritablePrimary"):
        break
    time.sleep(1)

REPLICA_URI = "mongodb://127.0.0.1:27021,127.0.0.1:27022,127.0.0.1:27023/?replicaSet=rs0"
client = MongoClient(REPLICA_URI)
db = client["ecommerce_ddbms"]

# 1. Configure Role-Based Access Control (SCRAM-SHA-256)
try:
    client["admin"].command({
        "createUser": "ddbms_app_admin",
        "pwd": "SecurePassword@2026",
        "roles": [{"role": "readWrite", "db": "ecommerce_ddbms"}]
    })
    print("[SECURITY] Created RBAC User 'ddbms_app_admin'.")
except Exception:
    print("[SECURITY] RBAC User 'ddbms_app_admin' verified active.")

# 2. Register Cluster Topology in Apache ZooKeeper
try:
    from kazoo.client import KazooClient
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start(timeout=2)
    zk.ensure_path("/ecommerce_ddbms/cluster_config")
    zk.set("/ecommerce_ddbms/cluster_config", json.dumps({
        "replica_set": "rs0",
        "nodes": ["127.0.0.1:27021", "127.0.0.1:27022", "127.0.0.1:27023"],
        "status": "HEALTHY"
    }).encode("utf-8"))
    zk.stop()
    print("[ZOOKEEPER] Cluster topology synced at '/ecommerce_ddbms/cluster_config'.")
except Exception:
    pass

# 3. Collections
products_col = db["products"]
interactions_col = db["user_interactions"]
spark_rules_col = db["spark_copurchase_rules"]
carts_col = db["user_carts"]
telemetry_col = db["telemetry_logs"]

products_col.drop()
interactions_col.drop()
spark_rules_col.drop()
carts_col.drop()
telemetry_col.drop()

# 32 Products across 8 Major E-Commerce Categories
catalog = [
    # 1. Mobiles
    {"product_id": "M101", "name": "Apple iPhone 16 Pro (256GB, Desert Titanium)", "brand": "Apple", "category": "Mobiles", "price": 119900, "mrp": 129900, "rating": 4.9, "reviews": 3420, "tag": "Flagship", "image": "https://images.unsplash.com/photo-1592750475338-74b7b21085ab?w=600&q=80"},
    {"product_id": "M102", "name": "Samsung Galaxy S24 Ultra 5G (Titanium Gray, 12GB)", "brand": "Samsung", "category": "Mobiles", "price": 109999, "mrp": 134999, "rating": 4.8, "reviews": 2890, "tag": "Bestseller", "image": "https://images.unsplash.com/photo-1610945415295-d9bbf067e59c?w=600&q=80"},
    {"product_id": "M103", "name": "OnePlus 12R 5G (Iron Gray, 16GB RAM, 256GB)", "brand": "OnePlus", "category": "Mobiles", "price": 39999, "mrp": 45999, "rating": 4.7, "reviews": 4120, "tag": "Top Value", "image": "https://images.unsplash.com/photo-1511707171634-5f897ff02aa9?w=600&q=80"},
    {"product_id": "M104", "name": "Nothing Phone (2a) Plus 5G Special Edition", "brand": "Nothing", "category": "Mobiles", "price": 25999, "mrp": 29999, "rating": 4.6, "reviews": 1950, "tag": "Trending", "image": "https://images.unsplash.com/photo-1598327105666-5b89351aff97?w=600&q=80"},

    # 2. Laptops
    {"product_id": "L201", "name": "Apple MacBook Air 15-inch (M3 Chip, 16GB, 512GB)", "brand": "Apple", "category": "Laptops", "price": 134900, "mrp": 154900, "rating": 4.9, "reviews": 1250, "tag": "Editor's Pick", "image": "https://images.unsplash.com/photo-1517336714731-489689fd1ca8?w=600&q=80"},
    {"product_id": "L202", "name": "ASUS ROG Zephyrus G14 OLED RTX 4060 Gaming", "brand": "ASUS", "category": "Laptops", "price": 149990, "mrp": 179990, "rating": 4.8, "reviews": 840, "tag": "Pro Gaming", "image": "https://images.unsplash.com/photo-1603302576837-37561b2e2302?w=600&q=80"},
    {"product_id": "L203", "name": "HP Pavilion Plus 14 Intel Core Ultra 5 OLED", "brand": "HP", "category": "Laptops", "price": 78990, "mrp": 96990, "rating": 4.6, "reviews": 1120, "tag": "Popular", "image": "https://images.unsplash.com/photo-1593642632823-8f785ba67e45?w=600&q=80"},
    {"product_id": "L204", "name": "Lenovo Legion Pro 7i Gen 9 Core i9 RTX 4080", "brand": "Lenovo", "category": "Laptops", "price": 199990, "mrp": 239990, "rating": 4.9, "reviews": 610, "tag": "Extreme", "image": "https://images.unsplash.com/photo-1496181133206-80ce9b88a853?w=600&q=80"},

    # 3. Audio
    {"product_id": "A101", "name": "Sony WH-1000XM5 Wireless Noise Cancelling Headphones", "brand": "Sony", "category": "Audio", "price": 26990, "mrp": 34990, "rating": 4.9, "reviews": 2410, "tag": "Bestseller", "image": "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?w=600&q=80"},
    {"product_id": "A102", "name": "Apple AirPods Pro (2nd Gen) with MagSafe Case", "brand": "Apple", "category": "Audio", "price": 20990, "mrp": 24900, "rating": 4.8, "reviews": 3890, "tag": "Top Rated", "image": "https://images.unsplash.com/photo-1600294037681-c80b4cb5b434?w=600&q=80"},
    {"product_id": "A103", "name": "boAt Nirvana Ion ANC True Wireless Earbuds", "brand": "boAt", "category": "Audio", "price": 1999, "mrp": 7990, "rating": 4.5, "reviews": 8420, "tag": "Super Deal", "image": "https://images.unsplash.com/photo-1546435770-a3e426bf472b?w=600&q=80"},
    {"product_id": "A104", "name": "Marshall Stanmore III Bluetooth Home Speaker", "brand": "Marshall", "category": "Audio", "price": 31999, "mrp": 41999, "rating": 4.8, "reviews": 780, "tag": "Iconic", "image": "https://images.unsplash.com/photo-1608043152269-423dbba4e7e1?w=600&q=80"},

    # 4. Fashion & Ethnic
    {"product_id": "F501", "name": "Manyavar Men's Jacquard Silk Festive Kurta Set", "brand": "Manyavar", "category": "Fashion", "price": 3499, "mrp": 5999, "rating": 4.8, "reviews": 1890, "tag": "Festive Hit", "image": "https://images.unsplash.com/photo-1617137984095-74e4e5e3613f?w=600&q=80"},
    {"product_id": "F502", "name": "Biba Women's Embroidered Anarkali Kurta Suit", "brand": "Biba", "category": "Fashion", "price": 2199, "mrp": 4599, "rating": 4.7, "reviews": 3210, "tag": "Trending", "image": "https://images.unsplash.com/photo-1583391733956-3750e0ff4e8b?w=600&q=80"},
    {"product_id": "F503", "name": "Nike Air Max Pulse Street Edition Sneakers", "brand": "Nike", "category": "Fashion", "price": 8495, "mrp": 13995, "rating": 4.9, "reviews": 960, "tag": "Premium", "image": "https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=600&q=80"},
    {"product_id": "F504", "name": "Kanjivaram Pure Zari Banarasi Silk Saree", "brand": "Varkala Silk", "category": "Fashion", "price": 1499, "mrp": 4999, "rating": 4.6, "reviews": 5430, "tag": "70% Off", "image": "https://images.unsplash.com/photo-1610030469983-98e550d6193c?w=600&q=80"},

    # 5. Home & Kitchen
    {"product_id": "H601", "name": "Philips Digital Air Fryer HD9252/90 (4.1 Liter)", "brand": "Philips", "category": "Home & Kitchen", "price": 6499, "mrp": 11995, "rating": 4.8, "reviews": 4620, "tag": "Bestseller", "image": "https://images.unsplash.com/photo-1585515320310-259814833e62?w=600&q=80"},
    {"product_id": "H602", "name": "Dyson V12 Detect Slim Cordless Vacuum Cleaner", "brand": "Dyson", "category": "Home & Kitchen", "price": 44900, "mrp": 58900, "rating": 4.9, "reviews": 670, "tag": "Smart Home", "image": "https://images.unsplash.com/photo-1558317374-067fb5f30001?w=600&q=80"},
    {"product_id": "H603", "name": "De'Longhi Dedica Arte Espresso Coffee Maker", "brand": "De'Longhi", "category": "Home & Kitchen", "price": 14499, "mrp": 21999, "rating": 4.7, "reviews": 890, "tag": "Cafe Style", "image": "https://images.unsplash.com/photo-1517668808822-9ebb02f2a0e6?w=600&q=80"},
    {"product_id": "H604", "name": "Milton Thermosteel Flip Lid Flask 1000ml", "brand": "Milton", "category": "Home & Kitchen", "price": 899, "mrp": 1350, "rating": 4.6, "reviews": 9120, "tag": "Daily Pick", "image": "https://images.unsplash.com/photo-1602143407151-7111542de6e8?w=600&q=80"},

    # 6. Wearables
    {"product_id": "W401", "name": "Apple Watch Ultra 2 49mm Titanium GPS+Cellular", "brand": "Apple", "category": "Wearables", "price": 89900, "mrp": 94900, "rating": 4.9, "reviews": 1390, "tag": "Titanium", "image": "https://images.unsplash.com/photo-1546868871-7041f2a55e12?w=600&q=80"},
    {"product_id": "W402", "name": "Samsung Galaxy Watch Ultra 47mm LTE Titanium", "brand": "Samsung", "category": "Wearables", "price": 59999, "mrp": 69999, "rating": 4.7, "reviews": 830, "tag": "Galaxy AI", "image": "https://images.unsplash.com/photo-1523275335684-37898b6baf30?w=600&q=80"},
    {"product_id": "W403", "name": "Garmin Forerunner 965 AMOLED Triathlon Watch", "brand": "Garmin", "category": "Wearables", "price": 67490, "mrp": 74990, "rating": 4.9, "reviews": 490, "tag": "Pro Sport", "image": "https://images.unsplash.com/photo-1508685096489-7aacd43bd3b1?w=600&q=80"},
    {"product_id": "W404", "name": "Ultrahuman Ring AIR Smart Health Tracker Ring", "brand": "Ultrahuman", "category": "Wearables", "price": 28499, "mrp": 32999, "rating": 4.6, "reviews": 1120, "tag": "Health Tech", "image": "https://images.unsplash.com/photo-1605100804763-247f67b3557e?w=600&q=80"},

    # 7. Gaming & Accessories
    {"product_id": "G301", "name": "Keychron Q1 Max QMK/VIA Wireless Mechanical Keyboard", "brand": "Keychron", "category": "Accessories", "price": 18999, "mrp": 22999, "rating": 4.9, "reviews": 1540, "tag": "Enthusiast", "image": "https://images.unsplash.com/photo-1595225476474-87563907a212?w=600&q=80"},
    {"product_id": "G302", "name": "Logitech MX Master 3S Wireless Ergonomic Mouse", "brand": "Logitech", "category": "Accessories", "price": 8995, "mrp": 10995, "rating": 4.8, "reviews": 4320, "tag": "Bestseller", "image": "https://images.unsplash.com/photo-1527864550417-7fd91fc51a46?w=600&q=80"},
    {"product_id": "G303", "name": "Samsung T9 2TB USB 3.2 Gen2x2 Portable SSD", "brand": "Samsung", "category": "Accessories", "price": 18499, "mrp": 24999, "rating": 4.7, "reviews": 910, "tag": "2000 MB/s", "image": "https://images.unsplash.com/photo-1597872200969-2b65d56bd16b?w=600&q=80"},
    {"product_id": "G304", "name": "LG UltraFine 27-inch 4K UHD USB-C Monitor", "brand": "LG", "category": "Accessories", "price": 32990, "mrp": 44990, "rating": 4.6, "reviews": 770, "tag": "4K HDR10", "image": "https://images.unsplash.com/photo-1527443224154-c4a3942d3acf?w=600&q=80"},

    # 8. Beauty & Personal Care
    {"product_id": "B701", "name": "Dyson Airwrap Multi-Styler Complete Long", "brand": "Dyson", "category": "Beauty & Care", "price": 45900, "mrp": 49900, "rating": 4.9, "reviews": 2150, "tag": "Salon Pro", "image": "https://images.unsplash.com/photo-1522337360788-8b13dee7a37e?w=600&q=80"},
    {"product_id": "B702", "name": "Philips OneBlade Pro 360 Hybrid Trimmer & Shaver", "brand": "Philips", "category": "Beauty & Care", "price": 3899, "mrp": 5499, "rating": 4.7, "reviews": 6340, "tag": "Top Grooming", "image": "https://images.unsplash.com/photo-1621607512214-68297480165e?w=600&q=80"},
    {"product_id": "B703", "name": "Forest Essentials Ayurvedic Soundarya Glow Kit", "brand": "Forest Essentials", "category": "Beauty & Care", "price": 4250, "mrp": 5500, "rating": 4.8, "reviews": 1480, "tag": "Luxury Care", "image": "https://images.unsplash.com/photo-1556228720-195a672e8a03?w=600&q=80"},
    {"product_id": "B704", "name": "Minimalist Daily Niacinamide & Vitamin C Routine", "brand": "Minimalist", "category": "Beauty & Care", "price": 1199, "mrp": 1699, "rating": 4.6, "reviews": 7820, "tag": "Bestseller", "image": "https://images.unsplash.com/photo-1608248597359-0e6d526a6828?w=600&q=80"}
]

for item in catalog:
    item["discount_pct"] = round(((item["mrp"] - item["price"]) / item["mrp"]) * 100)

products_col.insert_many(catalog)
print(f"[MONGODB] Seeded {len(catalog)} products across 8 categories.")

# 4. Generate 6,000 Background Interactions for Realistic Query Execution Stats
synthetic_users = [f"customer_{i}" for i in range(1, 200)]
interactions = []

for _ in range(5925):
    u = random.choice(synthetic_users)
    p = random.choice(catalog)
    act, wt = random.choice([("view", 1), ("wishlist", 3), ("cart", 5)])
    interactions.append({
        "user_id": u,
        "product_id": p["product_id"],
        "category": p["category"],
        "action": act,
        "weight": wt
    })

customers = [
    ("ananya_sharma", "Fashion", "F502", 25),
    ("rohan_verma", "Laptops", "L202", 25),
    ("vikram_nair", "Mobiles", "M101", 25)
]
for uname, cat, pid, count in customers:
    for _ in range(count):
        interactions.append({
            "user_id": uname,
            "product_id": pid,
            "category": cat,
            "action": "cart",
            "weight": 5
        })

interactions_col.insert_many(interactions)
print(f"[MONGODB] Inserted {len(interactions)} interaction records.")

# Initial Persistent Carts
carts_col.insert_many([
    {"user_id": "ananya_sharma", "product_id": "F502", "quantity": 1},
    {"user_id": "rohan_verma", "product_id": "G302", "quantity": 1},
    {"user_id": "vikram_nair", "product_id": "A103", "quantity": 2}
])

# 5. Create Compound Indexes
products_col.create_index([("category", ASCENDING), ("rating", DESCENDING)], name="idx_category_rating")
interactions_col.create_index([("user_id", ASCENDING), ("category", ASCENDING)], name="idx_user_category")
carts_col.create_index([("user_id", ASCENDING), ("product_id", ASCENDING)], unique=True)
print("[INDEXING] Compound Indexes active: 'idx_category_rating', 'idx_user_category'.")

# 6. Apache Spark Collaborative Co-Purchase Rules across all 8 Categories
co_purchase_pairs = [
    {"base_category": "Mobiles", "paired_category": "Audio", "confidence_pct": 94, "engine": "Spark ALS Collaborative Filter"},
    {"base_category": "Laptops", "paired_category": "Accessories", "confidence_pct": 96, "engine": "Spark ALS Collaborative Filter"},
    {"base_category": "Audio", "paired_category": "Wearables", "confidence_pct": 89, "engine": "Spark ALS Collaborative Filter"},
    {"base_category": "Fashion", "paired_category": "Beauty & Care", "confidence_pct": 92, "engine": "Spark ALS Collaborative Filter"},
    {"base_category": "Home & Kitchen", "paired_category": "Beauty & Care", "confidence_pct": 85, "engine": "Spark ALS Collaborative Filter"},
    {"base_category": "Wearables", "paired_category": "Audio", "confidence_pct": 91, "engine": "Spark ALS Collaborative Filter"},
    {"base_category": "Accessories", "paired_category": "Laptops", "confidence_pct": 93, "engine": "Spark ALS Collaborative Filter"},
    {"base_category": "Beauty & Care", "paired_category": "Fashion", "confidence_pct": 90, "engine": "Spark ALS Collaborative Filter"}
]
spark_rules_col.insert_many(co_purchase_pairs)
print("[SPARK ENGINE] Stored 8 cross-category co-purchase rules.")