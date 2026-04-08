import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

st.set_page_config(
    page_title="부동산 실거래가 분석",
    page_icon="🏠",
    layout="wide"
)

# ── 권역 매핑 ──
REGION_GROUP = {
    "서울_종로구": "서울_도심권", "서울_중구": "서울_도심권", "서울_용산구": "서울_도심권",
    "서울_성동구": "서울_동북권", "서울_광진구": "서울_동북권", "서울_동대문구": "서울_동북권",
    "서울_중랑구": "서울_동북권", "서울_성북구": "서울_동북권", "서울_강북구": "서울_동북권",
    "서울_도봉구": "서울_동북권", "서울_노원구": "서울_동북권",
    "서울_은평구": "서울_서북권", "서울_서대문구": "서울_서북권", "서울_마포구": "서울_서북권",
    "서울_양천구": "서울_서남권", "서울_강서구": "서울_서남권", "서울_구로구": "서울_서남권",
    "서울_금천구": "서울_서남권", "서울_영등포구": "서울_서남권", "서울_동작구": "서울_서남권",
    "서울_관악구": "서울_서남권",
    "서울_서초구": "서울_동남권", "서울_강남구": "서울_동남권", "서울_송파구": "서울_동남권",
    "서울_강동구": "서울_동남권",
    "경기_남양주시": "경기_동부", "경기_구리시": "경기_동부", "경기_하남시": "경기_동부",
    "경기_광주시": "경기_동부", "경기_이천시": "경기_동부", "경기_여주시": "경기_동부",
    "경기_가평군": "경기_동부", "경기_양평군": "경기_동부",
    "경기_김포시": "경기_서부", "경기_시흥시": "경기_서부", "경기_광명시": "경기_서부",
    "경기_안산시상록구": "경기_서부", "경기_안산시단원구": "경기_서부", "경기_부천시": "경기_서부",
    "경기_수원시장안구": "경기_남부", "경기_수원시권선구": "경기_남부", "경기_수원시팔달구": "경기_남부",
    "경기_수원시영통구": "경기_남부", "경기_오산시": "경기_남부", "경기_평택시": "경기_남부",
    "경기_안성시": "경기_남부", "경기_용인시처인구": "경기_남부", "경기_용인시기흥구": "경기_남부",
    "경기_용인시수지구": "경기_남부", "경기_화성시": "경기_남부",
    "경기_고양시덕양구": "경기_북부", "경기_고양시일산동구": "경기_북부", "경기_고양시일산서구": "경기_북부",
    "경기_파주시": "경기_북부", "경기_의정부시": "경기_북부", "경기_양주시": "경기_북부",
    "경기_동두천시": "경기_북부", "경기_포천시": "경기_북부", "경기_연천군": "경기_북부",
    "부산_동구": "부산_동부", "부산_동래구": "부산_동부", "부산_해운대구": "부산_동부",
    "부산_기장군": "부산_동부", "부산_금정구": "부산_동부", "부산_연제구": "부산_동부",
    "부산_수영구": "부산_동부",
    "부산_서구": "부산_서부", "부산_사하구": "부산_서부", "부산_강서구": "부산_서부",
    "부산_사상구": "부산_서부",
    "부산_남구": "부산_남부", "부산_영도구": "부산_남부",
    "부산_북구": "부산_북부",
    "부산_중구": "부산_중부", "부산_부산진구": "부산_중부",
}

SIZE_MAP_FILTER = {
    "small":       "소형 (~60㎡, ~18평)",
    "medium":      "중형 (60~85㎡, 18~26평)",
    "large":       "대형 (85~135㎡, 26~41평)",
    "extra_large": "초대형 (135㎡~, 41평~)"
}
SIZE_MAP_REVERSE = {v: k for k, v in SIZE_MAP_FILTER.items()}
SIZE_MAP_DISPLAY = {
    "small":       "소형(~60㎡)",
    "medium":      "중형(60~85㎡)",
    "large":       "대형(85~135㎡)",
    "extra_large": "초대형(135㎡~)"
}

def get_sido(region):
    return region.split("_")[0]

def get_group(region):
    return REGION_GROUP.get(region, None)

def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="realestate_db",
        user="realestate",
        password="realestate"
    )

@st.cache_data
def load_mart_data():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM dbt_realestate_mart.mart_price_dashboard", conn)
    conn.close()
    # 만원 → 억 변환
    for col in ["avg_trade_price", "min_trade_price", "max_trade_price", "avg_jeonse_price"]:
        if col in df.columns:
            df[col] = df[col] / 10000
    return df

@st.cache_data
def load_raw_trade():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT region_name, apt_name, deal_amount, area_sqm, floor,
               build_year, deal_year, deal_month, deal_day, umd_nm
        FROM public.apt_trade
    """, conn)
    conn.close()
    df["deal_amount"] = pd.to_numeric(df["deal_amount"], errors="coerce") / 10000
    return df

# ── 헤더 ──
st.title("🏠 전국 아파트 실거래가 분석 대시보드")
st.markdown("국토교통부 실거래가 공공 API 기반")
st.divider()

# ── 데이터 로드 ──
df = load_mart_data()
raw = load_raw_trade()

df["sido"] = df["region_name"].apply(get_sido)
df["group"] = df["region_name"].apply(get_group)
raw["sido"] = raw["region_name"].apply(get_sido)
raw["group"] = raw["region_name"].apply(get_group)
raw["deal_year"] = pd.to_numeric(raw["deal_year"], errors="coerce")
raw["deal_month"] = pd.to_numeric(raw["deal_month"], errors="coerce")

# ── 사이드바 필터 ──
st.sidebar.title("🔍 필터")

years = sorted(raw["deal_year"].dropna().unique().astype(int).tolist(), reverse=True)
year_options = ["전체"] + [str(y) for y in years]
selected_year = st.sidebar.selectbox("연도 선택", year_options)

if selected_year != "전체":
    months = sorted(raw[raw["deal_year"] == int(selected_year)]["deal_month"].dropna().unique().astype(int).tolist())
else:
    months = list(range(1, 13))
month_options = ["전체"] + [f"{m}월" for m in months]
selected_month_label = st.sidebar.selectbox("월 선택", month_options)
selected_month = int(selected_month_label.replace("월", "")) if selected_month_label != "전체" else "전체"

sidos = ["전체"] + sorted(df["sido"].unique().tolist())
selected_sido = st.sidebar.selectbox("광역시/도 선택", sidos)

selected_group = "전체"
if selected_sido in ["서울", "경기", "부산"]:
    groups = df[df["sido"] == selected_sido]["group"].dropna().unique().tolist()
    group_options = ["전체"] + sorted(groups)
    selected_group = st.sidebar.selectbox("권역 선택", group_options)

if selected_group != "전체":
    regions = ["전체"] + sorted(df[df["group"] == selected_group]["region_name"].unique().tolist())
elif selected_sido != "전체":
    regions = ["전체"] + sorted(df[df["sido"] == selected_sido]["region_name"].unique().tolist())
else:
    regions = ["전체"] + sorted(df["region_name"].unique().tolist())
selected_region = st.sidebar.selectbox("시군구 선택", regions)

size_options = ["전체"] + [SIZE_MAP_FILTER[s] for s in ["small", "medium", "large", "extra_large"]]
selected_size_label = st.sidebar.selectbox("평형 선택", size_options)
selected_size = SIZE_MAP_REVERSE.get(selected_size_label, "전체")

# ── 필터 적용 ──
filtered = df.copy()
raw_filtered = raw.copy()

if selected_year != "전체":
    raw_filtered = raw_filtered[raw_filtered["deal_year"] == int(selected_year)]
if selected_month != "전체":
    raw_filtered = raw_filtered[raw_filtered["deal_month"] == int(selected_month)]
if selected_sido != "전체":
    filtered = filtered[filtered["sido"] == selected_sido]
    raw_filtered = raw_filtered[raw_filtered["sido"] == selected_sido]
if selected_group != "전체":
    filtered = filtered[filtered["group"] == selected_group]
    raw_filtered = raw_filtered[raw_filtered["group"] == selected_group]
if selected_region != "전체":
    filtered = filtered[filtered["region_name"] == selected_region]
    raw_filtered = raw_filtered[raw_filtered["region_name"] == selected_region]
if selected_size != "전체":
    filtered = filtered[filtered["size_category"] == selected_size]

# ── KPI 카드 ──
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("총 거래건수", f"{int(filtered['trade_count'].sum()):,}건")
with col2:
    avg_price = filtered["avg_trade_price"].mean()
    st.metric("평균 매매가", f"{avg_price:.2f}억" if pd.notna(avg_price) else "N/A")
with col3:
    avg_jeonse = filtered["avg_jeonse_price"].mean()
    st.metric("평균 전세가", f"{avg_jeonse:.2f}억" if pd.notna(avg_jeonse) else "N/A")
with col4:
    avg_ratio = filtered["jeonse_ratio"].mean()
    st.metric("평균 전세가율", f"{avg_ratio:.1f}%" if pd.notna(avg_ratio) else "N/A")

st.divider()

# ── 차트 ──
col_left, col_right = st.columns(2)

with col_left:
    if selected_region != "전체":
        sido = get_sido(selected_region)
        grp = get_group(selected_region)
        if grp:
            compare_df = df[df["group"] == grp].groupby("region_name")["avg_trade_price"].mean().reset_index()
            title = f"📊 {grp} 내 구별 평균 매매가 비교 (억원)"
        else:
            compare_df = df[df["sido"] == sido].groupby("region_name")["avg_trade_price"].mean().reset_index()
            title = f"📊 {sido} 내 구별 평균 매매가 비교 (억원)"
        compare_df = compare_df.sort_values("avg_trade_price", ascending=True)
        compare_df["색상"] = compare_df["region_name"].apply(
            lambda x: "선택" if x == selected_region else "비교"
        )
        st.subheader(title)
        fig1 = px.bar(
            compare_df, x="avg_trade_price", y="region_name", orientation="h",
            color="색상",
            color_discrete_map={"선택": "#08519C", "비교": "#B8D8E8"},
            labels={"avg_trade_price": "평균 매매가(억원)", "region_name": "지역"}
        )
        fig1.update_layout(height=500, showlegend=False)
    else:
        chart_df = filtered.groupby("region_name")["avg_trade_price"].mean().reset_index()
        chart_df = chart_df.sort_values("avg_trade_price", ascending=True)
        st.subheader("📊 지역별 평균 매매가 (억원)")
        fig1 = px.bar(
            chart_df, x="avg_trade_price", y="region_name", orientation="h",
            color="avg_trade_price",
            color_continuous_scale=[[0, "#B8D8E8"], [0.5, "#6BAED6"], [1, "#08519C"]],
            labels={"avg_trade_price": "평균 매매가(억원)", "region_name": "지역"}
        )
        fig1.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)

with col_right:
    st.subheader("🍩 평형별 거래량 비중")
    size_count = filtered.groupby("size_category")["trade_count"].sum().reset_index()
    size_count["평형"] = size_count["size_category"].map(SIZE_MAP_DISPLAY)
    fig2 = px.pie(
        size_count, values="trade_count", names="평형",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig2.update_layout(height=500)
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

col_left2, col_right2 = st.columns(2)

with col_left2:
    if selected_region != "전체":
        sido = get_sido(selected_region)
        grp = get_group(selected_region)
        if grp:
            compare_df2 = df[df["group"] == grp].groupby("region_name")["jeonse_ratio"].mean().reset_index()
            title2 = f"📊 {grp} 내 전세가율 비교 (%)"
        else:
            compare_df2 = df[df["sido"] == sido].groupby("region_name")["jeonse_ratio"].mean().reset_index()
            title2 = f"📊 {sido} 내 전세가율 비교 (%)"
        compare_df2 = compare_df2.sort_values("jeonse_ratio", ascending=True)
        compare_df2["색상"] = compare_df2["region_name"].apply(
            lambda x: "선택" if x == selected_region else "비교"
        )
        st.subheader(title2)
        fig3 = px.bar(
            compare_df2, x="jeonse_ratio", y="region_name", orientation="h",
            color="색상",
            color_discrete_map={"선택": "#006D2C", "비교": "#C7E9C0"},
            labels={"jeonse_ratio": "전세가율(%)", "region_name": "지역"}
        )
        fig3.update_layout(height=500, showlegend=False)
    else:
        jeonse_df = filtered.groupby("region_name")["jeonse_ratio"].mean().reset_index()
        jeonse_df = jeonse_df.sort_values("jeonse_ratio", ascending=True)
        st.subheader("📊 지역별 전세가율 (%)")
        fig3 = px.bar(
            jeonse_df, x="jeonse_ratio", y="region_name", orientation="h",
            color="jeonse_ratio",
            color_continuous_scale=[[0, "#C7E9C0"], [0.5, "#74C476"], [1, "#006D2C"]],
            labels={"jeonse_ratio": "전세가율(%)", "region_name": "지역"}
        )
        fig3.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig3, use_container_width=True)

with col_right2:
    st.subheader("📊 매매가 분포 (억원)")
    hist_data = raw_filtered.copy()
    hist_data = hist_data.dropna(subset=["deal_amount"])
    if not hist_data.empty:
        fig4 = px.histogram(
            hist_data,
            x="deal_amount",
            nbins=30,
            color_discrete_sequence=["#6BAED6"],
            labels={"deal_amount": "매매가(억원)", "count": "거래건수"},
        )
        fig4.update_layout(
            height=500,
            bargap=0.05,
            xaxis_title="매매가 (억원)",
            yaxis_title="거래 건수"
        )
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("선택한 조건에 해당하는 매매 데이터가 없습니다.")

st.divider()

# ── 실거래 아파트 테이블 ──
st.subheader("📋 실거래 아파트 상세")

raw_filtered = raw_filtered.drop(columns=["sido", "group"], errors="ignore")
raw_filtered = raw_filtered.sort_values("deal_amount", ascending=False)
raw_filtered = raw_filtered.reset_index(drop=True)
raw_filtered.index = raw_filtered.index + 1
raw_filtered["deal_amount"] = raw_filtered["deal_amount"].apply(
    lambda x: f"{x:.2f}억" if pd.notna(x) else "-"
)

raw_filtered.columns = [
    "지역", "아파트명", "매매가", "전용면적(㎡)",
    "층", "건축년도", "계약년도", "계약월", "계약일", "법정동"
]

st.dataframe(raw_filtered, use_container_width=True)