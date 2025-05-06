import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client
from datetime import datetime

st.set_page_config(layout="wide")

# --- Supabase Setup ---
@st.cache_resource
def get_supabase():
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)

supabase = get_supabase()

# --- Data Loaders ---
@st.cache_data
def load_traceability():
    page_size = 1000
    offset = 0
    all_rows = []

    while True:
        result = supabase.table("traceability").select("*").range(offset, offset + page_size - 1).execute()
        rows = result.data
        if not rows:
            break
        all_rows.extend(rows)
        offset += page_size

    return pd.DataFrame(all_rows)


@st.cache_data
def load_quota_view():
    page_size = 1000
    offset = 0
    all_rows = []
    while True:
        result = supabase.table("quota_view").select("*").range(offset, offset + page_size - 1).execute()
        rows = result.data
        if not rows:
            break
        all_rows.extend(rows)
        offset += page_size
    return pd.DataFrame(all_rows)

@st.cache_data
def load_farmers():
    page_size = 1000
    offset = 0
    all_rows = []
    while True:
        result = supabase.table("farmers").select("*").range(offset, offset + page_size - 1).execute()
        rows = result.data
        if not rows:
            break
        all_rows.extend(rows)
        offset += page_size
    return pd.DataFrame(all_rows)

# --- Load data ---
trace_df = load_traceability()
quota_df = load_quota_view()
farmers_df = load_farmers()

# --- Preprocess ---
trace_df['purchase_date'] = pd.to_datetime(trace_df['purchase_date'], errors='coerce')
trace_df['net_weight_kg'] = pd.to_numeric(trace_df['net_weight_kg'], errors='coerce')
quota_df['quota_used_pct'] = pd.to_numeric(quota_df['quota_used_pct'], errors='coerce')
trace_df['farmer_id'] = trace_df['farmer_id'].astype(str).str.strip().str.lower()
farmers_df['farmer_id'] = farmers_df['farmer_id'].astype(str).str.strip().str.lower()

# --- Filter 1: Exporter ---
# --- Filter 1: Exporter ---
trace_df['exporter'] = trace_df['exporter'].astype(str).str.strip()
exporters = trace_df['exporter'].replace('', pd.NA).dropna().unique()
selected_exporters = st.sidebar.multiselect("Select Exporter", exporters, default=list(exporters))
trace_df_filtered = trace_df[trace_df['exporter'].isin(selected_exporters)]


# If empty after exporter filter
if trace_df_filtered.empty:
    st.warning("⚠️ No traceability data for the selected exporter(s).")
    st.stop()

# --- Merge area info ---
trace_df_filtered = trace_df_filtered.merge(
    farmers_df[['farmer_id', 'area_ha']],
    on='farmer_id',
    how='left'
)

# --- Filter 2: Area ---
st.sidebar.subheader("Filter by Farm Area (ha)")
if trace_df_filtered['area_ha'].notna().any():
    min_area = float(trace_df_filtered['area_ha'].min())
    max_area = float(trace_df_filtered['area_ha'].max())
    selected_area_min = st.sidebar.slider("Min Area", min_value=0.0, max_value=max_area, value=min_area)
    trace_df_filtered = trace_df_filtered[trace_df_filtered['area_ha'] >= selected_area_min]
else:
    st.warning("⚠️ No area data available. Area filter disabled.")
    selected_area_min = None

if trace_df_filtered.empty:
    st.warning("⚠️ No data left after applying area filter.")
    st.stop()

# --- Main Dashboard ---
st.title("📊 CloudIA Reporting Dashboard")

# --- KPI cards ---
total_net_weight = trace_df_filtered['net_weight_kg'].sum()
total_deliveries = len(trace_df_filtered)
total_area = trace_df_filtered['area_ha'].sum()

col1, col2, col3 = st.columns(3)
col1.metric("📦 Total Net Weight (kg)", f"{total_net_weight:,.0f}")
col2.metric("🚚 Total Deliveries", f"{total_deliveries:,}")
col3.metric("🌾 Total Area (ha)", f"{total_area:,.2f}")

# --- Farmer Coverage Comparison ---
st.subheader("👩‍🌾 Farmer Coverage Summary")

total_farmers_in_farmers = farmers_df['farmer_id'].nunique()
total_farmers_in_trace = trace_df['farmer_id'].nunique()

col4, col5, col6 = st.columns(3)
col4.metric("🧾 Farmers in Traceability", f"{total_farmers_in_trace:,}")
col5.metric("📋 Farmers in Farmers Table", f"{total_farmers_in_farmers:,}")
col6.metric("📈 Coverage (%)", f"{(total_farmers_in_trace / total_farmers_in_farmers * 100):.1f}%" if total_farmers_in_farmers else "N/A")


# --- Trend chart ---
st.subheader("📈 Net Weight Over Time")
# Filtruj dane od 2024 roku
filtered_for_chart = trace_df_filtered[trace_df_filtered['purchase_date'].dt.year >= 2024]

# Grupuj tylko dane od 2024 roku
weight_over_time = filtered_for_chart.groupby(filtered_for_chart['purchase_date'].dt.date)['net_weight_kg'].sum().reset_index()

chart = alt.Chart(weight_over_time).mark_line().encode(
    x='purchase_date:T',
    y='net_weight_kg:Q'
).properties(width=800, height=300)
st.altair_chart(chart, use_container_width=True)

# --- Quota Status Pie ---
st.subheader("✅ Quota Compliance Status")
quota_status_count = quota_df['quota_status'].value_counts().reset_index()
quota_status_count.columns = ['quota_status', 'count']
pie_chart = alt.Chart(quota_status_count).mark_arc().encode(
    theta='count:Q',
    color='quota_status:N',
    tooltip=['quota_status', 'count']
).properties(width=300, height=300)
st.altair_chart(pie_chart)

# --- Top Farmers ---
st.subheader("🏅 Top 10 Farmers by Net Weight")
top_farmers = trace_df_filtered.groupby('farmer_id')['net_weight_kg'].sum().sort_values(ascending=False).head(10).reset_index()
st.bar_chart(top_farmers.set_index('farmer_id'))

# --- Certification Share ---
st.subheader("🏷️ Certification Distribution (%)")

cert_counts = trace_df_filtered['certification'].value_counts(normalize=True).reset_index()
cert_counts.columns = ['certification', 'percentage']
cert_counts['percentage'] *= 100  # convert to percent

cert_chart = alt.Chart(cert_counts).mark_bar().encode(
    x=alt.X('certification:N', title='Certification'),
    y=alt.Y('percentage:Q', title='Percentage'),
    tooltip=['certification', alt.Tooltip('percentage:Q', format='.1f')]
).properties(width=600, height=300)

st.altair_chart(cert_chart, use_container_width=True)

# --- Certified Volumes by Certification ---
st.subheader("📦 Certified Volumes (in MT)")

volume_by_cert = trace_df_filtered.groupby('certification')['net_weight_kg'].sum().reset_index()
volume_by_cert['volume_mt'] = (volume_by_cert['net_weight_kg'] / 1000).round(2)
volume_by_cert = volume_by_cert[['certification', 'volume_mt']].sort_values(by='volume_mt', ascending=False)

st.dataframe(volume_by_cert, use_container_width=True)



# --- Full Table View ---
st.subheader("📋 Full Traceability Data")
st.dataframe(trace_df_filtered, use_container_width=True)

# --- Export CSV ---
st.download_button("📥 Download CSV", trace_df_filtered.to_csv(index=False), "traceability_data.csv", "text/csv")

