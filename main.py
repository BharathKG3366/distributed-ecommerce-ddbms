import time
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient, ReadPreference, ASCENDING, DESCENDING
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern

app = FastAPI(title="ElectroMart Distributed E-Commerce Platform")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

REPLICA_URI = "mongodb://127.0.0.1:27021,127.0.0.1:27022,127.0.0.1:27023/?replicaSet=rs0"
client = MongoClient(REPLICA_URI)
db = client["ecommerce_ddbms"]

class InteractionRequest(BaseModel):
    user_id: str
    product_id: str
    category: str
    action: str
    consistency_mode: str = "BASE"

class CartUpdateRequest(BaseModel):
    user_id: str
    product_id: str
    delta: int  # +1, -1, or 0 (remove item)

@app.get("/", response_class=HTMLResponse)
def home():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/catalog")
def get_catalog(
    category: str = "All",
    search: str = "",
    price_range: str = "ALL",
    min_rating: float = 0.0,
    min_discount: int = 0,
    sort_by: str = "rating"
):
    col = db.get_collection("products", read_preference=ReadPreference.SECONDARY_PREFERRED)
    query = {}
    if category != "All":
        query["category"] = category
    if search.strip():
        query["$or"] = [
            {"name": {"$regex": search.strip(), "$options": "i"}},
            {"brand": {"$regex": search.strip(), "$options": "i"}},
            {"category": {"$regex": search.strip(), "$options": "i"}}
        ]
    if price_range == "UNDER_5K":
        query["price"] = {"$lte": 5000}
    elif price_range == "5K_25K":
        query["price"] = {"$gt": 5000, "$lte": 25000}
    elif price_range == "25K_75K":
        query["price"] = {"$gt": 25000, "$lte": 75000}
    elif price_range == "ABOVE_75K":
        query["price"] = {"$gt": 75000}

    if min_rating > 0:
        query["rating"] = {"$gte": min_rating}
    if min_discount > 0:
        query["discount_pct"] = {"$gte": min_discount}

    sort_field, sort_dir = ("rating", DESCENDING)
    if sort_by == "price_asc":
        sort_field, sort_dir = ("price", ASCENDING)
    elif sort_by == "price_desc":
        sort_field, sort_dir = ("price", DESCENDING)
    elif sort_by == "discount":
        sort_field, sort_dir = ("discount_pct", DESCENDING)
    elif sort_by == "reviews":
        sort_field, sort_dir = ("reviews", DESCENDING)

    products = list(col.find(query, {"_id": 0}).sort(sort_field, sort_dir))
    return {"products": products, "count": len(products)}

@app.post("/api/interact")
def log_interaction(req: InteractionRequest):
    t0 = time.perf_counter()
    weights = {"view": 1, "wishlist": 3, "cart": 5}

    if req.consistency_mode == "BASE":
        inter_col = db.get_collection("user_interactions", write_concern=WriteConcern(w=1))
        cart_col = db.get_collection("user_carts", write_concern=WriteConcern(w=1))
        desc = "BASE (WriteConcern w=1 | Async Secondary Sync)"
    else:
        inter_col = db.get_collection("user_interactions", write_concern=WriteConcern(w="majority", j=True))
        cart_col = db.get_collection("user_carts", write_concern=WriteConcern(w="majority", j=True))
        desc = "ACID Quorum (WriteConcern w='majority', j=True)"

    inter_col.insert_one({
        "user_id": req.user_id,
        "product_id": req.product_id,
        "category": req.category,
        "action": req.action,
        "weight": weights.get(req.action, 1),
        "timestamp": datetime.now().strftime("%H:%M:%S")
    })

    if req.action == "cart":
        cart_col.update_one(
            {"user_id": req.user_id, "product_id": req.product_id},
            {"$inc": {"quantity": 1}},
            upsert=True
        )

    latency = round((time.perf_counter() - t0) * 1000, 2)
    return {"status": "ok", "write_latency_ms": latency, "mode_used": desc}

@app.get("/api/cart/{user_id}")
def get_user_cart(user_id: str):
    cart_docs = list(db["user_carts"].find({"user_id": user_id}, {"_id": 0}))
    if not cart_docs:
        return {"items": [], "total_items": 0, "total_mrp": 0, "total_price": 0, "total_savings": 0, "qty_map": {}}

    pids = [c["product_id"] for c in cart_docs]
    prod_map = {p["product_id"]: p for p in db["products"].find({"product_id": {"$in": pids}}, {"_id": 0})}

    detailed_items = []
    qty_map = {}
    total_qty = 0
    total_mrp = 0
    total_price = 0

    for c in cart_docs:
        p = prod_map.get(c["product_id"])
        if p and c["quantity"] > 0:
            qty = c["quantity"]
            qty_map[p["product_id"]] = qty
            subtotal = p["price"] * qty
            sub_mrp = p["mrp"] * qty
            total_qty += qty
            total_price += subtotal
            total_mrp += sub_mrp
            detailed_items.append({
                "product_id": p["product_id"],
                "name": p["name"],
                "brand": p["brand"],
                "category": p["category"],
                "image": p["image"],
                "price": p["price"],
                "mrp": p["mrp"],
                "quantity": qty,
                "subtotal": subtotal
            })

    return {
        "items": detailed_items,
        "total_items": total_qty,
        "total_mrp": total_mrp,
        "total_price": total_price,
        "total_savings": total_mrp - total_price,
        "qty_map": qty_map
    }

@app.post("/api/cart/update")
def update_cart_item(req: CartUpdateRequest):
    if req.delta == 0:
        db["user_carts"].delete_one({"user_id": req.user_id, "product_id": req.product_id})
    else:
        existing = db["user_carts"].find_one({"user_id": req.user_id, "product_id": req.product_id})
        if existing:
            new_qty = existing["quantity"] + req.delta
            if new_qty <= 0:
                db["user_carts"].delete_one({"user_id": req.user_id, "product_id": req.product_id})
            else:
                db["user_carts"].update_one(
                    {"user_id": req.user_id, "product_id": req.product_id},
                    {"$set": {"quantity": new_qty}}
                )
        elif req.delta > 0:
            db["user_carts"].insert_one({"user_id": req.user_id, "product_id": req.product_id, "quantity": req.delta})
    return get_user_cart(req.user_id)

@app.delete("/api/cart/clear/{user_id}")
def clear_cart(user_id: str):
    db["user_carts"].delete_many({"user_id": user_id})
    return get_user_cart(user_id)

@app.post("/api/affinity/reset/{user_id}")
def reset_user_affinity(user_id: str, mode: str = "ZERO"):
    db["user_interactions"].delete_many({"user_id": user_id})
    if mode == "DEFAULT":
        defaults = {
            "ananya_sharma": ("Fashion", "F502"),
            "rohan_verma": ("Laptops", "L202"),
            "vikram_nair": ("Mobiles", "M101")
        }
        cat, pid = defaults.get(user_id, ("Mobiles", "M101"))
        db["user_interactions"].insert_many([
            {"user_id": user_id, "product_id": pid, "category": cat, "action": "cart", "weight": 5}
            for _ in range(25)
        ])
    return {"status": "reset", "user_id": user_id, "mode": mode}

@app.delete("/api/telemetry/clear")
def clear_telemetry_logs():
    db["telemetry_logs"].delete_many({})
    return {"status": "cleared"}

@app.get("/api/recommendations/{user_id}")
def get_recommendations(user_id: str, consistency_mode: str = "BASE", indexed: bool = True):
    t0 = time.perf_counter()
    prod_admin = db["products"]
    inter_admin = db["user_interactions"]

    if indexed:
        prod_admin.create_index([("category", ASCENDING), ("rating", DESCENDING)], name="idx_category_rating")
        inter_admin.create_index([("user_id", ASCENDING), ("category", ASCENDING)], name="idx_user_category")
    else:
        try:
            prod_admin.drop_index("idx_category_rating")
            inter_admin.drop_index("idx_user_category")
        except Exception:
            pass

    if consistency_mode == "BASE":
        read_pref = ReadPreference.SECONDARY_PREFERRED
        read_conc = ReadConcern("local")
        routed_node = "Secondary Replica (27022 / 27023)"
    else:
        read_pref = ReadPreference.PRIMARY
        read_conc = ReadConcern("majority")
        routed_node = "Primary Coordinator (27021)"

    inter_col = db.get_collection("user_interactions", read_preference=read_pref, read_concern=read_conc)
    prod_col = db.get_collection("products", read_preference=read_pref, read_concern=read_conc)

    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$group": {"_id": "$category", "score": {"$sum": "$weight"}, "events": {"$sum": 1}}},
        {"$sort": {"score": -1}},
        {"$limit": 4}
    ]
    affinity = list(inter_col.aggregate(pipeline))
    top_cat = affinity[0]["_id"] if affinity else "Mobiles"

    spark_rule = db["spark_copurchase_rules"].find_one({"base_category": top_cat}, {"_id": 0})
    paired_cat = spark_rule["paired_category"] if spark_rule else "Audio"

    inter_explain = inter_col.find({"user_id": user_id}).explain().get("executionStats", {})
    prod_explain = prod_col.find({"category": top_cat}).sort("rating", -1).explain()
    prod_stats = prod_explain.get("executionStats", {})
    winning_plan_str = str(prod_explain.get("queryPlanner", {}).get("winningPlan", {}))
    scan_stage = "IXSCAN (Compound Index)" if "IXSCAN" in winning_plan_str else "COLLSCAN (Full Scan)"

    primary_recs = list(prod_col.find({"category": top_cat}, {"_id": 0}).sort("rating", -1).limit(3))
    cross_recs = list(prod_col.find({"category": paired_cat}, {"_id": 0}).sort("rating", -1).limit(1))
    for item in cross_recs:
        if spark_rule:
            item["spark_badge"] = f"Frequently Bought Together ({spark_rule['confidence_pct']}%)"

    recommendations = primary_recs + cross_recs
    roundtrip_ms = round((time.perf_counter() - t0) * 1000, 2)

    telemetry_snapshot = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "user_id": user_id,
        "consistency_model": consistency_mode,
        "routed_node": routed_node,
        "scan_stage": scan_stage,
        "interaction_docs_examined": inter_explain.get("totalDocsExamined", 0),
        "interaction_keys_examined": inter_explain.get("totalKeysExamined", 0),
        "product_docs_examined": prod_stats.get("totalDocsExamined", 0),
        "mongo_exec_ms": inter_explain.get("executionTimeMillis", 0) + prod_stats.get("executionTimeMillis", 0),
        "total_api_ms": roundtrip_ms
    }

    db["telemetry_logs"].insert_one(dict(telemetry_snapshot))
    recent_logs = list(db["telemetry_logs"].find({}, {"_id": 0}).sort("$natural", -1).limit(5))

    return {
        "recommendations": recommendations,
        "affinity": affinity,
        "spark_insight": spark_rule,
        "telemetry": telemetry_snapshot,
        "telemetry_history": recent_logs
    }