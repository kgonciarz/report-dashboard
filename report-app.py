import streamlit as st
import pandas as pd
import altair as alt
from supabase import create_client
from datetime import datetime
from postgrest.exceptions import APIError


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


from postgrest.exceptions import APIError

@st.cache_data(show_spinner=False)
def load_quota_view():
    page_size = 1000
    offset = 0
    all_rows = []
    cols = "quota_status,quota_used_pct"

    while True:
        try:
            result = (
                supabase
                .table("quota_view")
                .select(cols)
                .range(offset, offset + page_size - 1)
                .execute()
            )
        except APIError as e:
            payload = e.args[0] if e.args else None
            st.error("Failed to load quota_view (PostgREST APIError).")
            st.write("Error payload:", payload)
            st.write("Full exception:", repr(e))
            st.stop()

        rows = result.data or []
        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < page_size:
            break

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

@st.cache_data
def get_total_traceability_count():
    result = supabase.table("traceability").select("id", count="exact").execute()
    return result.count

# --- Helpers & sanity check ---
def ensure_col(df, col, default):
    if col not in df.columns:
        df[col] = default
    return df

# Optional debug toggle
if st.sidebar.checkbox("Show raw columns/head (debug)"):
    st.write("trace_df columns:", list(trace_df.columns)); st.write(trace_df.head())
    st.write("quota_df columns:", list(quota_df.columns)); st.write(quota_df.head())
    st.write("farmers_df columns:", list(farmers_df.columns)); st.write(farmers_df.head())

# Optional: quick button to clear cached data during debugging
if st.sidebar.button("Clear data cache"):
    st.cache_data.clear()

# --- Load data ---
trace_df = load_traceability()
quota_df = load_quota_view()
farmers_df = load_farmers()

# --- Preprocess ---
# --- Preprocess (robust) ---
# Make sure required columns exist even if Supabase rows omitted keys
trace_df = ensure_col(trace_df, 'purchase_date', pd.NaT)
trace_df = ensure_col(trace_df, 'net_weight_kg', pd.NA)
trace_df = ensure_col(trace_df, 'farmer_id', pd.NA)
trace_df = ensure_col(trace_df, 'exporter', pd.NA)
trace_df = ensure_col(trace_df, 'certification', pd.NA)

quota_df = ensure_col(quota_df, 'quota_used_pct', pd.NA)
quota_df = ensure_col(quota_df, 'quota_status', pd.NA)

farmers_df = ensure_col(farmers_df, 'farmer_id', pd.NA)
farmers_df = ensure_col(farmers_df, 'area_ha', pd.NA)

# Coerce types safely
trace_df['purchase_date'] = pd.to_datetime(trace_df['purchase_date'], errors='coerce')
trace_df['net_weight_kg'] = pd.to_numeric(trace_df['net_weight_kg'], errors='coerce')
quota_df['quota_used_pct'] = pd.to_numeric(quota_df['quota_used_pct'], errors='coerce')

# Normalize IDs
trace_df['farmer_id'] = trace_df['farmer_id'].astype(str).str.strip().str.lower()
farmers_df['farmer_id'] = farmers_df['farmer_id'].astype(str).str.strip().str.lower()
farmers_df = farmers_df.drop_duplicates(subset='farmer_id')



# --- Filter 1: Exporter ---
# --- Filter 1: Exporter (multi-exporter aware) ---
# --- Filter 1: Exporter (multi-exporter aware, robust) ---
all_exporters = (
    trace_df['exporter']
    .dropna()
    .astype(str)
    .str.split(r",\s*")         # split on commas
)

flat_exporters = sorted({e.strip() for sub in all_exporters for e in sub if e.strip()})
selected_exporters = st.sidebar.multiselect(
    "Select Exporter",
    options=flat_exporters,
    default=flat_exporters if flat_exporters else []
)

def matches_any_exporter(val: str) -> bool:
    if not selected_exporters:   # if nothing to filter by, keep all rows
        return True
    if pd.isna(val):
        return False
    s = str(val)
    return any(sel in s for sel in selected_exporters)

trace_df_filtered = trace_df[trace_df['exporter'].apply(matches_any_exporter)]


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
total_net_weight = trace_df['net_weight_kg'].sum()
total_deliveries = len(trace_df)
total_area = trace_df_filtered['area_ha'].sum()

col1, col2, col3 = st.columns(3)
col1.metric("📦 Total Net Weight (kg)", f"{total_net_weight:,.0f}")
col2.metric("🚚 Total Deliveries", f"{get_total_traceability_count():,}")
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
# --- Trend chart ---
st.subheader("📈 Net Weight Over Time")

if trace_df_filtered['purchase_date'].notna().any():
    filtered_for_chart = trace_df_filtered[trace_df_filtered['purchase_date'].dt.year >= 2024]
    if not filtered_for_chart.empty:
        weight_over_time = (
            filtered_for_chart
            .groupby(filtered_for_chart['purchase_date'].dt.date, dropna=True)['net_weight_kg']
            .sum()
            .reset_index()
            .rename(columns={'purchase_date': 'date'})
        )
        chart = alt.Chart(weight_over_time).mark_line().encode(
            x=alt.X('date:T', title='Purchase Date'),
            y=alt.Y('net_weight_kg:Q', title='Net Weight (kg)')
        ).properties(width=800, height=300)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No records from 2024 onward to chart.")
else:
    st.info("No valid purchase dates to chart.")


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

