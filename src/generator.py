"""
Enterprise E-Commerce & Supply Chain Logistics Data Generator.
Big Data Analyst Portfolio Edition.

Generates 1,000,000+ orders and ~1,500,000 line items across a realistic Star Schema:
- Dimensions: customers (50k), products (1k), warehouses (10 hubs)
- Fact Tables: orders (1M header transactions) and order_items (~1.5M line items)

Features Top 1% Real-World Logistics Dynamics:
1. Snapshot Shipping Address Grain (decouples transaction address from moving customer profiles)
2. Vectorized Haversine distance calculations between warehouse hubs and delivery destinations
3. Causal delay attribution: separates Warehouse Dispatch SLA (2 days) from Carrier Transit SLA
4. Realistic OTIF (On-Time In-Full) metric calculation
5. Injected data quality quirks: customer segment typos, missing dates on cancellations, 1% duplicate transactions
"""

import numpy as np
import pandas as pd
import os
from pathlib import Path
from datetime import datetime, timedelta

# ==============================================================================
# 1. SETUP & DIRECTORIES
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "bronze"
RANDOM_SEED = 42

for entity in ["customers", "products", "warehouses", "orders", "order_items"]:
    os.makedirs(DATA_DIR / entity, exist_ok=True)

# Helper function for vectorized random dates
def random_dates(start_date, end_date, n):
    delta = end_date - start_date
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_seconds = np.random.randint(0, int_delta, n)
    return [start_date + timedelta(seconds=int(s)) for s in random_seconds]

# Vectorized Haversine distance calculation in kilometers
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0 # Earth's radius in km
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2
    c = 2.0 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    return R * c

# City/State coordinate lookup table for realistic distance modeling
GEO_COORDINATES = {
    ("New York", "NY"): (40.7128, -74.0060),
    ("Chicago", "IL"): (41.8781, -87.6298),
    ("Dallas", "TX"): (32.7767, -96.7970),
    ("Los Angeles", "CA"): (34.0522, -118.2437),
    ("Atlanta", "GA"): (33.7490, -84.3880),
    ("Seattle", "WA"): (47.6062, -122.3321),
    ("Phoenix", "AZ"): (33.4484, -112.0740),
    ("Denver", "CO"): (39.7392, -104.9903),
    ("Kansas City", "MO"): (39.0997, -94.5786),
    ("Philadelphia", "PA"): (39.9526, -75.1652),
    ("Houston", "TX"): (29.7604, -95.3698),
    ("Miami", "FL"): (25.7617, -80.1918),
    ("Columbus", "OH"): (39.9612, -82.9988),
    ("Charlotte", "NC"): (35.2271, -80.8431),
    ("Detroit", "MI"): (42.3314, -83.0458),
}

# ==============================================================================
# 2. GENERATE DIMENSION TABLES (Customers, Products, Warehouses)
# ==============================================================================
def generate_dimensions(n_cust=50_000, n_prod=1_000):
    np.random.seed(RANDOM_SEED)
    print(f"Generating Dimensions: {n_cust:,} Customers, {n_prod:,} Products, 10 Warehouses...")
    
    # ---------------------------------------------------------
    # A. Warehouses Dimension (10 Major Distribution Hubs)
    # ---------------------------------------------------------
    warehouses_data = [
        (1, "East Coast Hub", "New York", "NY", 500_000),
        (2, "Midwest Logistics Center", "Chicago", "IL", 750_000),
        (3, "Southern Gateway", "Dallas", "TX", 600_000),
        (4, "Pacific Distribution Hub", "Los Angeles", "CA", 800_000),
        (5, "Southeast Express Hub", "Atlanta", "GA", 450_000),
        (6, "Northwest Fulfillment", "Seattle", "WA", 400_000),
        (7, "Southwest Center", "Phoenix", "AZ", 350_000),
        (8, "Rocky Mountain Hub", "Denver", "CO", 300_000),
        (9, "Central Plains Hub", "Kansas City", "MO", 350_000),
        (10, "Mid-Atlantic Depot", "Philadelphia", "PA", 400_000)
    ]
    df_warehouses = pd.DataFrame(warehouses_data, columns=[
        "warehouse_id", "warehouse_name", "city", "state", "capacity_units"
    ])
    df_warehouses.to_csv(DATA_DIR / "warehouses" / "warehouses.csv", index=False)
    
    # ---------------------------------------------------------
    # B. Products Dimension (1,000 SKUs across 6 Categories)
    # ---------------------------------------------------------
    categories = ["Electronics", "Home & Kitchen", "Apparel", "Books", "Sports & Outdoors", "Beauty & Health"]
    cat_weights = [0.25, 0.25, 0.20, 0.10, 0.10, 0.10]
    
    prod_ids = np.arange(1001, 1001 + n_prod)
    prod_cats = np.random.choice(categories, size=n_prod, p=cat_weights)
    
    base_prices = {
        "Electronics": (80.0, 1200.0),
        "Home & Kitchen": (15.0, 350.0),
        "Apparel": (12.0, 180.0),
        "Books": (8.0, 60.0),
        "Sports & Outdoors": (20.0, 450.0),
        "Beauty & Health": (10.0, 120.0)
    }
    
    unit_prices = []
    unit_costs = []
    weights_kg = []
    
    for cat in prod_cats:
        low, high = base_prices[cat]
        price = round(float(np.random.uniform(low, high)), 2)
        margin = np.random.uniform(0.30, 0.65) # 30% to 65% profit margins
        cost = round(price * (1.0 - margin), 2)
        weight = round(float(np.random.gamma(shape=2.0, scale=1.2)), 2)
        weight = max(0.20, min(weight, 25.0))
        
        unit_prices.append(price)
        unit_costs.append(cost)
        weights_kg.append(weight)
        
    df_products = pd.DataFrame({
        "product_id": prod_ids,
        "product_name": [f"{c} Item #{pid}" for c, pid in zip(prod_cats, prod_ids)],
        "category": prod_cats,
        "unit_price": unit_prices,
        "unit_cost": unit_costs,
        "weight_kg": weights_kg
    })
    df_products.to_csv(DATA_DIR / "products" / "products.csv", index=False)
    
    # ---------------------------------------------------------
    # C. Customers Dimension (50,000 Customers with Injected Typos)
    # ---------------------------------------------------------
    cust_ids = np.arange(50001, 50001 + n_cust)
    geo_keys = list(GEO_COORDINATES.keys())
    geo_indices = np.random.randint(0, len(geo_keys), size=n_cust)
    cust_cities = [geo_keys[i][0] for i in geo_indices]
    cust_states = [geo_keys[i][1] for i in geo_indices]
    
    segments = ["Consumer", "Corporate", "Small Business", "Consumr", "Corp"]
    seg_probs = [0.65, 0.20, 0.10, 0.03, 0.02]
    
    df_customers = pd.DataFrame({
        "customer_id": cust_ids,
        "customer_name": [f"Customer_{cid}" for cid in cust_ids],
        "city": cust_cities,
        "state": cust_states,
        "signup_date": [d.strftime('%Y-%m-%d') for d in random_dates(datetime(2020, 1, 1), datetime(2022, 12, 31), n_cust)],
        "customer_segment": np.random.choice(segments, size=n_cust, p=seg_probs)
    })
    df_customers.to_csv(DATA_DIR / "customers" / "customers.csv", index=False)
    print("✅ Dimension tables saved successfully.")
    
    return df_products, df_customers, df_warehouses

# ==============================================================================
# 3. GENERATE FACT TABLES: ORDERS (HEADER) & ORDER_ITEMS (LINE ITEMS)
# ==============================================================================
def generate_chunk(chunk_idx, start_order_id, chunk_size, seed, df_products, df_customers, df_warehouses):
    np.random.seed(seed)
    print(f"  -> Generating Chunk {chunk_idx + 1}: Orders {start_order_id:,} to {start_order_id + chunk_size - 1:,}...")
    
    wh_coords = {
        row["warehouse_id"]: (GEO_COORDINATES[(row["city"], row["state"])][0], GEO_COORDINATES[(row["city"], row["state"])][1])
        for _, row in df_warehouses.iterrows()
    }
    cust_locs = df_customers.set_index("customer_id")[["city", "state"]].to_dict(orient="index")
    prod_data = df_products.set_index("product_id")[["unit_price", "weight_kg"]].to_dict(orient="index")
    prod_ids_arr = df_products["product_id"].values
    
    order_ids = np.arange(start_order_id, start_order_id + chunk_size)
    cust_choices = np.random.choice(df_customers["customer_id"].values, size=chunk_size)
    wh_choices = np.random.choice(df_warehouses["warehouse_id"].values, size=chunk_size)
    
    # 1. Snapshot Shipping Address Grain
    all_geo_keys = list(GEO_COORDINATES.keys())
    alt_geo_indices = np.random.randint(0, len(all_geo_keys), size=chunk_size)
    ship_to_alt = np.random.rand(chunk_size) < 0.05
    
    ship_cities, ship_states, dest_lats, dest_lons = [], [], [], []
    for cid, alt_idx, is_alt in zip(cust_choices, alt_geo_indices, ship_to_alt):
        if is_alt:
            c, s = all_geo_keys[alt_idx]
        else:
            c = cust_locs[cid]["city"]
            s = cust_locs[cid]["state"]
        lat, lon = GEO_COORDINATES[(c, s)]
        ship_cities.append(c)
        ship_states.append(s)
        dest_lats.append(lat)
        dest_lons.append(lon)
        
    wh_lats = np.array([wh_coords[w][0] for w in wh_choices])
    wh_lons = np.array([wh_coords[w][1] for w in wh_choices])
    distances_km = haversine_km(wh_lats, wh_lons, np.array(dest_lats), np.array(dest_lons))
    
    start_dt = datetime(2022, 1, 1)
    end_dt = datetime(2024, 6, 30)
    order_dates = random_dates(start_dt, end_dt, chunk_size)
    
    # Vectorized Shipping Fee & Promised Days Allocation
    shipping_fees = np.random.choice([0.0, 4.99, 9.99, 14.99], size=chunk_size, p=[0.40, 0.35, 0.15, 0.10])
    promised_days = np.zeros(chunk_size, dtype=int)
    for fee, days_choices, p_dist in [
        (14.99, [2, 3], [0.60, 0.40]),
        (9.99,  [3, 4], [0.70, 0.30]),
        (4.99,  [4, 5], [0.65, 0.35]),
        (0.00,  [5, 7], [0.60, 0.40])
    ]:
        mask = (shipping_fees == fee)
        promised_days[mask] = np.random.choice(days_choices, size=np.sum(mask), p=p_dist)
            
    statuses = np.random.choice(["Completed", "Cancelled", "Returned"], size=chunk_size, p=[0.88, 0.08, 0.04])
    
    # 2. Generate Order Items
    items_per_order = np.random.choice([1, 2, 3], size=chunk_size, p=[0.65, 0.25, 0.10])
    total_line_items = np.sum(items_per_order)
    repeated_order_ids = np.repeat(order_ids, items_per_order)
    selected_prods = np.random.choice(prod_ids_arr, size=total_line_items)
    quantities = np.random.choice([1, 2, 3, 4, 5], size=total_line_items, p=[0.60, 0.22, 0.10, 0.05, 0.03])
    discounts = np.random.choice([0.0, 0.05, 0.10, 0.15, 0.20], size=total_line_items, p=[0.55, 0.20, 0.15, 0.07, 0.03])
    
    unit_prices_items = np.array([prod_data[p]["unit_price"] for p in selected_prods])
    item_weights = np.array([prod_data[p]["weight_kg"] for p in selected_prods]) * quantities
    line_totals = np.round(quantities * unit_prices_items * (1.0 - discounts), 2)
    
    df_temp_items = pd.DataFrame({"order_id": repeated_order_ids, "weight": item_weights})
    order_weights = df_temp_items.groupby("order_id", sort=True)["weight"].sum().values
    
    item_start_id = start_order_id * 10
    df_chunk_items = pd.DataFrame({
        "order_item_id": np.arange(item_start_id, item_start_id + total_line_items),
        "order_id": repeated_order_ids,
        "product_id": selected_prods,
        "quantity": quantities,
        "unit_price": unit_prices_items,
        "discount_pct": discounts,
        "line_total": line_totals
    })
    
    # 3. Logistics Delay Attribution
    hub_congestion = np.isin(wh_choices, [1, 4]).astype(int)
    q4_surge = np.array([1 if d.month in [11, 12] else 0 for d in order_dates])
    dispatch_days_raw = np.random.poisson(lam=1.3, size=chunk_size) + (hub_congestion * np.random.choice([0, 1], size=chunk_size, p=[0.70, 0.30])) + (q4_surge * np.random.choice([0, 1], size=chunk_size, p=[0.75, 0.25]))
    dispatch_days = np.clip(dispatch_days_raw, 1, 6)
    is_dispatch_delayed = (dispatch_days > 2).astype(int)
    
    distance_days = distances_km / 800.0
    weight_friction = np.clip(order_weights / 15.0, 0, 2.0)
    transit_noise = np.random.choice([-1, 0, 1, 2], size=chunk_size, p=[0.20, 0.55, 0.20, 0.05])
    transit_days = np.clip(np.round(distance_days + weight_friction + transit_noise).astype(int), 1, 15)
    
    carrier_sla_window = np.maximum(promised_days - 2, 1)
    is_transit_delayed = (transit_days > carrier_sla_window).astype(int)
    
    actual_delivery_days = dispatch_days + transit_days
    is_delayed = (actual_delivery_days > promised_days).astype(int)
    
    ship_dates = [o_date + timedelta(days=int(d_days)) for o_date, d_days in zip(order_dates, dispatch_days)]
    delivery_dates = [s_date + timedelta(days=int(t_days)) for s_date, t_days in zip(ship_dates, transit_days)]
    
    formatted_order_dates = [d.strftime('%Y-%m-%d') for d in order_dates]
    formatted_ship_dates = [s.strftime('%Y-%m-%d') if st != "Cancelled" else None for s, st in zip(ship_dates, statuses)]
    formatted_delivery_dates = [deliv.strftime('%Y-%m-%d') if st in ["Completed", "Returned"] else None for deliv, st in zip(delivery_dates, statuses)]
    
    is_physically_delivered = np.isin(statuses, ["Completed", "Returned"])
    actual_delivery_days_clean = np.where(is_physically_delivered, actual_delivery_days, np.nan)
    transit_days_clean = np.where(is_physically_delivered, transit_days, np.nan)
    is_dispatch_delayed_clean = np.where(statuses == "Cancelled", 0, is_dispatch_delayed)
    is_transit_delayed_clean = np.where(is_physically_delivered, is_transit_delayed, 0)
    is_delayed_clean = np.where(is_physically_delivered, is_delayed, 0)
    
    is_otif = np.where((is_delayed_clean == 0) & (statuses == "Completed"), 1, 0)
    
    df_chunk_orders = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": cust_choices,
        "warehouse_id": wh_choices,
        "order_date": formatted_order_dates,
        "ship_date": formatted_ship_dates,
        "delivery_date": formatted_delivery_dates,
        "promised_days": promised_days,
        "dispatch_days": np.where(statuses != "Cancelled", dispatch_days, np.nan),
        "transit_days": transit_days_clean,
        "actual_delivery_days": actual_delivery_days_clean,
        "shipping_city": ship_cities,
        "shipping_state": ship_states,
        "shipping_fee": shipping_fees,
        "order_status": statuses,
        "is_dispatch_delayed": is_dispatch_delayed_clean,
        "is_transit_delayed": is_transit_delayed_clean,
        "is_delayed": is_delayed_clean,
        "is_otif": is_otif
    })
    
    # Injected 1% duplicate orders (for PySpark deduplication testing)
    dup_sample = df_chunk_orders.sample(frac=0.01, random_state=seed)
    dup_items = df_chunk_items[df_chunk_items["order_id"].isin(dup_sample["order_id"])]
    df_chunk_orders = pd.concat([df_chunk_orders, dup_sample], ignore_index=True)
    df_chunk_items = pd.concat([df_chunk_items, dup_items], ignore_index=True)
    
    return df_chunk_orders, df_chunk_items

# ==============================================================================
# 4. EXECUTION CONTROLLER
# ==============================================================================
def generate_all_data(total_orders=1_000_000, chunk_size=250_000):
    df_products, df_customers, df_warehouses = generate_dimensions()
    
    num_chunks = total_orders // chunk_size
    print(f"\nWriting {total_orders:,} Orders across {num_chunks} chunks to:\n{DATA_DIR}\n")
    
    total_generated_orders = 0
    total_generated_items = 0
    
    for i in range(num_chunks):
        start_order_id = 10_000_000 + (i * chunk_size)
        df_orders_chunk, df_items_chunk = generate_chunk(
            chunk_idx=i,
            start_order_id=start_order_id,
            chunk_size=chunk_size,
            seed=RANDOM_SEED + i,
            df_products=df_products,
            df_customers=df_customers,
            df_warehouses=df_warehouses
        )
        
        out_orders_file = DATA_DIR / "orders" / f"orders_part_{i}.csv"
        df_orders_chunk.to_csv(out_orders_file, index=False)
        
        out_items_file = DATA_DIR / "order_items" / f"order_items_part_{i}.csv"
        df_items_chunk.to_csv(out_items_file, index=False)
        
        total_generated_orders += len(df_orders_chunk)
        total_generated_items += len(df_items_chunk)
        print(f"    Saved {out_orders_file.name} ({len(df_orders_chunk):,} rows, {out_orders_file.stat().st_size / (1024*1024):.1f} MB)")
        print(f"    Saved {out_items_file.name} ({len(df_items_chunk):,} rows, {out_items_file.stat().st_size / (1024*1024):.1f} MB)\n")
        
    print("=" * 70)
    print(f"🎉 High-Volume Data Generation Completed Successfully!")
    print(f"   Total Orders Generated (with duplicates):     {total_generated_orders:,}")
    print(f"   Total Order Items Generated (with dups):     {total_generated_items:,}")
    print("=" * 70)

if __name__ == "__main__":
    generate_all_data()
