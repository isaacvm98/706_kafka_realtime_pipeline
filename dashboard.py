import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Spread Management Dashboard", 
    layout="wide", 
    page_icon="📊"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .alert-box {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .alert-warning {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
    }
    .alert-success {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-header">📊 Real-Time Spread Management Dashboard</p>', unsafe_allow_html=True)

DATABASE_URL = "postgresql://kafka_user:kafka_password@postgres:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_market_events(symbols: list = None, limit: int = 5000) -> pd.DataFrame:
    """Load raw market events"""
    base_query = "SELECT * FROM market_events WHERE 1=1"
    params = {}
    
    if symbols and len(symbols) > 0:
        placeholders = ','.join([f':symbol_{i}' for i in range(len(symbols))])
        base_query += f" AND symbol IN ({placeholders})"
        for i, symbol in enumerate(symbols):
            params[f'symbol_{i}'] = symbol
    
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params['limit'] = limit

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(base_query), conn, params=params)
        return df
    except Exception as e:
        st.error(f"Error loading events: {e}")
        return pd.DataFrame()

def load_spread_metrics(symbols: list = None, limit: int = 500) -> pd.DataFrame:
    """Load Flink-generated spread metrics"""
    base_query = "SELECT * FROM spread_metrics WHERE 1=1"
    params = {}
    
    if symbols and len(symbols) > 0:
        placeholders = ','.join([f':symbol_{i}' for i in range(len(symbols))])
        base_query += f" AND symbol IN ({placeholders})"
        for i, symbol in enumerate(symbols):
            params[f'symbol_{i}'] = symbol
    
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params['limit'] = limit

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(base_query), conn, params=params)
        return df
    except Exception as e:
        return pd.DataFrame()

# Sidebar Configuration
st.sidebar.header("⚙️ Dashboard Controls")

try:
    with engine.connect() as conn:
        all_symbols = pd.read_sql_query(
            text("SELECT DISTINCT symbol FROM market_events ORDER BY symbol"), 
            conn
        )['symbol'].tolist()
except:
    all_symbols = ["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA"]

selected_symbols = st.sidebar.multiselect(
    "📈 Select Symbols",
    options=all_symbols,
    default=all_symbols[:5] if len(all_symbols) >= 5 else all_symbols
)

update_interval = st.sidebar.slider(
    "🔄 Update Interval (seconds)", 
    min_value=2, 
    max_value=30, 
    value=5
)

limit_records = st.sidebar.number_input(
    "📊 Events to Load", 
    min_value=500, 
    max_value=20000, 
    value=5000, 
    step=500
)

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

# Main dashboard loop
placeholder = st.empty()

while True:
    df_events = load_market_events(symbols=selected_symbols, limit=int(limit_records))
    df_metrics = load_spread_metrics(symbols=selected_symbols, limit=500)

    with placeholder.container():
        if df_events.empty:
            st.warning("⚠️ No data available. Ensure producer and consumer are running.")
            time.sleep(update_interval)
            continue

        # Convert timestamps
        if "timestamp" in df_events.columns:
            df_events["timestamp"] = pd.to_datetime(df_events["timestamp"])
        if not df_metrics.empty and "timestamp" in df_metrics.columns:
            df_metrics["timestamp"] = pd.to_datetime(df_metrics["timestamp"])

        # ========== KEY METRICS ROW ==========
        st.markdown("### 📊 Market Overview")
        
        total_events = len(df_events)
        avg_spread = df_events["quoted_spread_bps"].mean()
        min_spread = df_events["quoted_spread_bps"].min()
        max_spread = df_events["quoted_spread_bps"].max()
        avg_stress = df_events["market_stress_level"].mean()
        
        # Count anomalies and optimal windows from metrics
        if not df_metrics.empty:
            anomaly_count = df_metrics["spread_anomaly"].sum()
            optimal_windows = df_metrics["optimal_execution_window"].sum()
        else:
            anomaly_count = 0
            optimal_windows = 0

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("📈 Total Events", f"{total_events:,}")
        col2.metric("📏 Avg Spread", f"{avg_spread:.2f} bps")
        col3.metric("📊 Spread Range", f"{min_spread:.1f} - {max_spread:.1f} bps")
        col4.metric("😰 Market Stress", f"{avg_stress:.2%}")
        col5.metric("⚠️ Anomalies", f"{anomaly_count}")

        st.markdown("---")

        # ========== ALERTS ROW ==========
        if not df_metrics.empty:
            st.markdown("### 🚨 Real-Time Alerts")
            
            alert_col1, alert_col2 = st.columns(2)
            
            with alert_col1:
                # Spread anomalies
                anomalies = df_metrics[df_metrics["spread_anomaly"] == True].sort_values("timestamp", ascending=False).head(5)
                if not anomalies.empty:
                    st.markdown('<div class="alert-box alert-warning"><b>⚠️ Wide Spread Alerts</b></div>', unsafe_allow_html=True)
                    for _, row in anomalies.iterrows():
                        st.write(f"**{row['symbol']}**: {row['avg_quoted_spread_bps']:.2f} bps "
                                f"(±{row['std_quoted_spread_bps']:.2f}) - "
                                f"{row['timestamp'].strftime('%H:%M:%S')}")
                else:
                    st.info("✅ No spread anomalies detected")
            
            with alert_col2:
                # Optimal execution windows
                optimal = df_metrics[df_metrics["optimal_execution_window"] == True].sort_values("timestamp", ascending=False).head(5)
                if not optimal.empty:
                    st.markdown('<div class="alert-box alert-success"><b>🎯 Optimal Execution Windows</b></div>', unsafe_allow_html=True)
                    for _, row in optimal.iterrows():
                        st.write(f"**{row['symbol']}**: {row['avg_quoted_spread_bps']:.2f} bps "
                                f"| Liquidity: {row['liquidity_score']:.1f} - "
                                f"{row['timestamp'].strftime('%H:%M:%S')}")
                else:
                    st.info("⏳ Waiting for optimal execution conditions")

        st.markdown("---")

        # ========== CHARTS ROW 1: Spread Timeline & Distribution ==========
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            st.markdown("#### 📈 Spread Evolution Over Time")
            
            # Time series of spreads by symbol
            spread_timeseries = df_events.groupby(['symbol', pd.Grouper(key='timestamp', freq='1min')]).agg({
                'quoted_spread_bps': 'mean'
            }).reset_index()
            
            fig_spread_ts = px.line(
                spread_timeseries,
                x='timestamp',
                y='quoted_spread_bps',
                color='symbol',
                title='Average Quoted Spread (1-minute intervals)',
                labels={'quoted_spread_bps': 'Spread (bps)', 'timestamp': 'Time'}
            )
            fig_spread_ts.update_layout(height=350, hovermode='x unified')
            st.plotly_chart(fig_spread_ts, use_container_width=True, key="spread_timeseries_chart")

        with chart_col2:
            st.markdown("#### 📊 Spread Distribution by Symbol")
            
            fig_spread_box = px.box(
                df_events,
                x='symbol',
                y='quoted_spread_bps',
                title='Spread Distribution',
                labels={'quoted_spread_bps': 'Spread (bps)'},
                color='symbol'
            )
            fig_spread_box.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig_spread_box, use_container_width=True, key="spread_distribution_chart")

        # ========== CHARTS ROW 2: Liquidity & Market Stress ==========
        chart_col3, chart_col4 = st.columns(2)

        with chart_col3:
            st.markdown("#### 💧 Liquidity Score (Real-Time)")
            
            if not df_metrics.empty:
                latest_metrics = df_metrics.sort_values('timestamp').groupby('symbol').last().reset_index()
                
                fig_liquidity = go.Figure()
                fig_liquidity.add_trace(go.Bar(
                    x=latest_metrics['symbol'],
                    y=latest_metrics['liquidity_score'],
                    marker_color='lightblue',
                    text=latest_metrics['liquidity_score'].round(1),
                    textposition='outside'
                ))
                fig_liquidity.update_layout(
                    height=350,
                    xaxis_title="Symbol",
                    yaxis_title="Liquidity Score (1000/spread_bps)",
                    title="Higher = More Liquid"
                )
                st.plotly_chart(fig_liquidity, use_container_width=True, key="liquidity_score_chart")
            else:
                st.info("Waiting for Flink metrics...")

        with chart_col4:
            st.markdown("#### 😰 Market Stress Levels")
            
            stress_timeseries = df_events.groupby(pd.Grouper(key='timestamp', freq='1min')).agg({
                'market_stress_level': 'mean'
            }).reset_index()
            
            fig_stress = go.Figure()
            fig_stress.add_trace(go.Scatter(
                x=stress_timeseries['timestamp'],
                y=stress_timeseries['market_stress_level'],
                fill='tozeroy',
                line=dict(color='red', width=2),
                name='Market Stress'
            ))
            fig_stress.update_layout(
                height=350,
                xaxis_title="Time",
                yaxis_title="Stress Level",
                title="Market Stress (0=Calm, 1=Crisis)",
                yaxis=dict(range=[0, 1])
            )
            st.plotly_chart(fig_stress, use_container_width=True, key="market_stress_chart")

        # ========== CHARTS ROW 3: Flink Metrics Analysis ==========
        if not df_metrics.empty:
            st.markdown("#### 🔬 Flink Windowed Analytics (1-Minute Windows)")
            
            chart_col5, chart_col6 = st.columns(2)
            
            with chart_col5:
                st.markdown("**Quote-to-Trade Ratio by Symbol**")
                
                latest_qtt = df_metrics.sort_values('timestamp').groupby('symbol').last().reset_index()
                
                fig_qtt = go.Figure()
                fig_qtt.add_trace(go.Bar(
                    x=latest_qtt['symbol'],
                    y=latest_qtt['quote_to_trade_ratio'],
                    marker_color='steelblue',
                    text=latest_qtt['quote_to_trade_ratio'].round(2),
                    textposition='outside'
                ))
                fig_qtt.update_layout(
                    height=300,
                    xaxis_title="Symbol",
                    yaxis_title="QTT Ratio",
                    title="Higher = More Quote Activity"
                )
                st.plotly_chart(fig_qtt, use_container_width=True, key="qtt_ratio_chart")
            
            with chart_col6:
                st.markdown("**Order Imbalance Ratio**")
                
                fig_imbalance = go.Figure()
                fig_imbalance.add_trace(go.Bar(
                    x=latest_qtt['symbol'],
                    y=latest_qtt['order_imbalance_ratio'],
                    marker_color=latest_qtt['order_imbalance_ratio'].apply(
                        lambda x: 'green' if x > 0.5 else 'red'
                    ),
                    text=latest_qtt['order_imbalance_ratio'].round(3),
                    textposition='outside'
                ))
                fig_imbalance.add_hline(y=0.5, line_dash="dash", line_color="gray", annotation_text="Balanced")
                fig_imbalance.update_layout(
                    height=300,
                    xaxis_title="Symbol",
                    yaxis_title="Buy Ratio",
                    title="Green=Buy Pressure, Red=Sell Pressure",
                    yaxis=dict(range=[0, 1])
                )
                st.plotly_chart(fig_imbalance, use_container_width=True, key="order_imbalance_chart")

        # ========== METRICS TABLE ==========
        if not df_metrics.empty:
            st.markdown("### 📋 Latest Windowed Metrics (Flink Output)")
            
            display_metrics = df_metrics.sort_values('timestamp', ascending=False).head(20)
            display_cols = [
                'timestamp', 'symbol', 'avg_quoted_spread_bps', 'std_quoted_spread_bps',
                'liquidity_score', 'quote_to_trade_ratio', 'order_imbalance_ratio',
                'spread_anomaly', 'optimal_execution_window'
            ]
            
            st.dataframe(
                display_metrics[display_cols].style.format({
                    'avg_quoted_spread_bps': '{:.2f}',
                    'std_quoted_spread_bps': '{:.2f}',
                    'liquidity_score': '{:.2f}',
                    'quote_to_trade_ratio': '{:.2f}',
                    'order_imbalance_ratio': '{:.3f}'
                }).applymap(
                    lambda x: 'background-color: #ffcccc' if x == True else '', 
                    subset=['spread_anomaly']
                ).applymap(
                    lambda x: 'background-color: #ccffcc' if x == True else '', 
                    subset=['optimal_execution_window']
                ),
                use_container_width=True,
                height=400
            )

        # ========== RAW EVENTS TABLE ==========
        st.markdown("### 📋 Recent Market Events")
        
        display_cols = [
            'timestamp', 'symbol', 'event_type', 'side', 'price',
            'bid_price', 'ask_price', 'quoted_spread_bps', 'market_stress_level'
        ]
        
        st.dataframe(
            df_events[display_cols].head(20).style.format({
                'price': '${:.2f}',
                'bid_price': '${:.2f}',
                'ask_price': '${:.2f}',
                'quoted_spread_bps': '{:.2f}',
                'market_stress_level': '{:.2%}'
            }),
            use_container_width=True,
            height=400
        )

        # ========== FOOTER ==========
        st.markdown("---")
        
        metrics_age = ""
        if not df_metrics.empty:
            latest_metric_time = df_metrics['timestamp'].max()
            age_seconds = (datetime.now() - latest_metric_time).total_seconds()
            metrics_age = f"| Flink metrics age: {age_seconds:.0f}s"
        
        st.caption(
            f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
            f"Auto-refresh: {update_interval}s | "
            f"Events: {total_events:,} {metrics_age}"
        )

    time.sleep(update_interval)