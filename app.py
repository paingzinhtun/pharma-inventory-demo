import streamlit as st
import pandas as pd
import sqlite3
import random
from datetime import datetime, timedelta

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="JK AI Innovation | Pharma Dashboard", layout="wide")

# --- PART 1: HELPER FUNCTIONS (Data Generation) ---
def generate_fake_data():
    """Generates the messy 'Yangon' and 'Mandalay' datasets on the fly."""
    products = [
        {"id": "P001", "name": "Amoxicillin 500mg"},
        {"id": "P002", "name": "Paracetamol 500mg"},
        {"id": "P003", "name": "Cetirizine 10mg"},
        {"id": "P004", "name": "Vitamin C 1000mg"},
        {"id": "P005", "name": "Omeprazole 20mg"}
    ]
    
    # Generate Yangon (Clean-ish)
    ygn_data = []
    for _ in range(50):
        prod = random.choice(products)
        days = random.choice([-10, 30, 60, 365])
        date_str = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        ygn_data.append({
            "Product_ID": prod["id"], "Product_Name": prod["name"], 
            "Batch_No": f"YGN-{random.randint(1000,9999)}", 
            "Expiry_Date": date_str, "Stock_Qty": random.randint(50, 500), 
            "Warehouse_Loc": "Yangon_Main"
        })
        
    # Generate Mandalay (Messy)
    mdl_data = []
    for _ in range(50):
        prod = random.choice(products)
        days = random.choice([10, 90, 120])
        # Intentional dirty format: DD/MM/YYYY
        date_str = (datetime.now() + timedelta(days=days)).strftime("%d/%m/%Y")
        mdl_data.append({
            "PID": prod["id"], "Name": prod["name"], 
            "Batch": f"MDL-{random.randint(1000,9999)}", 
            "Exp_Date": date_str, "Qty": random.randint(20, 200), 
            "Location": "Mandalay_Branch"
        })
        
    return pd.DataFrame(ygn_data), pd.DataFrame(mdl_data)

# --- PART 2: ETL PIPELINE ---
def run_etl_pipeline(df_ygn, df_mdl):
    """Cleans and merges the two dataframes."""
    
    # 1. Clean Yangon
    df_ygn_clean = df_ygn.rename(columns={
        "Product_ID": "product_id", "Product_Name": "product_name",
        "Batch_No": "batch_no", "Expiry_Date": "expiry_date",
        "Stock_Qty": "quantity", "Warehouse_Loc": "branch_location"
    })
    df_ygn_clean["expiry_date"] = pd.to_datetime(df_ygn_clean["expiry_date"])
    
    # 2. Clean Mandalay
    df_mdl_clean = df_mdl.rename(columns={
        "PID": "product_id", "Name": "product_name",
        "Batch": "batch_no", "Qty": "quantity", 
        "Location": "branch_location"
    })
    # Fix DD/MM/YYYY format
    df_mdl_clean["expiry_date"] = pd.to_datetime(df_mdl["Exp_Date"], dayfirst=True)
    df_mdl_clean = df_mdl_clean.drop(columns=["Exp_Date"], errors='ignore') # cleanup
    
    # 3. Merge
    df_master = pd.concat([df_ygn_clean, df_mdl_clean], ignore_index=True)
    
    # 4. Business Logic (Status)
    now = pd.Timestamp.now()
    df_master["days_until_expiry"] = (df_master["expiry_date"] - now).dt.days
    
    def get_status(d):
        if d < 0: return "EXPIRED"
        elif d < 90: return "CRITICAL"
        else: return "HEALTHY"
        
    df_master["status"] = df_master["days_until_expiry"].apply(get_status)
    return df_master

# --- PART 3: THE APP UI ---

st.title("💊 Royal Thanlwin Pharma (Demo)")
st.caption("Powered by JK AI Innovation Data Pipeline")

# Sidebar for controls
with st.sidebar:
    st.header("Control Panel")
    if st.button("🔄 Regenerate Live Data"):
        st.session_state['data_generated'] = True
        df_ygn, df_mdl = generate_fake_data()
        st.session_state['df_master'] = run_etl_pipeline(df_ygn, df_mdl)
        st.success("New data pulled from branches!")

# Check if data exists in session, if not, generate it
if 'df_master' not in st.session_state:
    df_ygn, df_mdl = generate_fake_data()
    st.session_state['df_master'] = run_etl_pipeline(df_ygn, df_mdl)

df = st.session_state['df_master']

# --- DASHBOARD ROW 1: KPI METRICS ---
col1, col2, col3 = st.columns(3)
expired_count = df[df['status'] == 'EXPIRED']['quantity'].sum()
critical_count = df[df['status'] == 'CRITICAL']['quantity'].sum()
total_stock = df['quantity'].sum()

col1.metric("Total Inventory", f"{total_stock} Units")
col2.metric("At Risk (Critical)", f"{critical_count} Units", delta_color="off")
col3.metric("Already Expired (Loss)", f"{expired_count} Units", delta_color="inverse")

st.divider()

# --- DASHBOARD ROW 2: SQL QUERY RESULTS ---

# Setup SQLite in memory for queries
conn = sqlite3.connect(":memory:")
df.to_sql("inventory", conn, index=False, if_exists="replace")

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("🔥 Fire Sale List (Expires < 90 Days)")
    st.write("Action: Send this list to Sales Team immediately.")
    
    query_fire = """
    SELECT product_name, batch_no, branch_location, expiry_date, days_until_expiry 
    FROM inventory 
    WHERE status = 'CRITICAL' 
    ORDER BY days_until_expiry ASC
    """
    df_fire = pd.read_sql(query_fire, conn)
    # Format date for display
    df_fire['expiry_date'] = pd.to_datetime(df_fire['expiry_date']).dt.strftime('%Y-%m-%d')
    st.dataframe(df_fire, use_container_width=True)

with col_right:
    st.subheader("📊 Risk by Branch")
    query_chart = """
    SELECT branch_location, COUNT(*) as batch_count 
    FROM inventory 
    WHERE status IN ('CRITICAL', 'EXPIRED') 
    GROUP BY branch_location
    """
    df_chart = pd.read_sql(query_chart, conn)
    st.bar_chart(df_chart, x="branch_location", y="batch_count")

# --- DEBUG VIEW (Show Raw Data) ---
with st.expander("Show Raw Data Logic (For Engineers)"):
    st.write("This is the cleaned master dataset used for analysis:")
    st.dataframe(df)