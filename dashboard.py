import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Database Connection
DB_URL = "postgresql://airflow:airflow@localhost:5435/hackernews"

st.set_page_config(
    page_title="HN Analytics Dashboard", 
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="collapsed"
)

# Initialize theme in session state
if 'theme' not in st.session_state:
    st.session_state.theme = 'light'

# Theme toggle in sidebar
with st.sidebar:
    st.title("⚙️ Settings")
    theme_choice = st.radio(
        "Theme",
        options=['light', 'dark'],
        index=0 if st.session_state.theme == 'light' else 1,
        horizontal=True
    )
    if theme_choice != st.session_state.theme:
        st.session_state.theme = theme_choice
        st.rerun()

# Theme configurations
if st.session_state.theme == 'light':
    THEMES = {
        'bg': '#ffffff',
        'secondary_bg': '#f8f9fa',
        'text': '#1a1a1a',
        'text_secondary': '#718096',
        'border': '#e2e8f0',
        'card_bg': 'linear-gradient(145deg, #ffffff, #f7fafc)',
        'chart_bg': 'white',
        'metric_value': '#1a1a1a',
        'metric_label': '#718096',
        'gradient_start': '#667eea',
        'gradient_end': '#764ba2',
    }
else:
    THEMES = {
        'bg': '#1a1a1a',
        'secondary_bg': '#2d3748',
        'text': '#f7fafc',
        'text_secondary': '#a0aec0',
        'border': '#4a5568',
        'card_bg': 'linear-gradient(145deg, #2d3748, #1a1a1a)',
        'chart_bg': '#2d3748',
        'metric_value': '#f7fafc',
        'metric_label': '#a0aec0',
        'gradient_start': '#667eea',
        'gradient_end': '#764ba2',
    }

# Dynamic CSS based on theme
st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    * {{
        font-family: 'Inter', sans-serif;
    }}
    
    /* Force theme colors */
    .main, .stApp, [data-testid="stAppViewContainer"] {{
        background-color: {THEMES['bg']} !important;
    }}
    
    [data-testid="stHeader"] {{
        background-color: {THEMES['bg']} !important;
    }}
    
    .block-container {{
        padding-top: 3rem;
        padding-bottom: 3rem;
        background-color: {THEMES['bg']} !important;
    }}
    
    [data-testid="stSidebar"] {{
        background-color: {THEMES['secondary_bg']} !important;
    }}
    
    /* Headers */
    h1, h2, h3 {{
        color: {THEMES['text']} !important;
    }}
    
    h1 {{
        font-weight: 700;
        font-size: 3rem !important;
        margin-bottom: 0.5rem !important;
        letter-spacing: -0.02em;
    }}
    
    h2 {{
        font-weight: 600;
        font-size: 1.5rem !important;
        margin-top: 2rem !important;
        margin-bottom: 1rem !important;
    }}
    
    /* Text colors */
    .main p, .main span, .main div {{
        color: {THEMES['text_secondary']} !important;
    }}
    
    /* Metric cards */
    [data-testid="stMetricValue"] {{
        font-size: 2rem;
        font-weight: 700;
        color: {THEMES['metric_value']} !important;
    }}
    
    [data-testid="stMetricLabel"] {{
        color: {THEMES['metric_label']} !important;
        font-weight: 500;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}
    
    div[data-testid="stMetric"] {{
        background: {THEMES['card_bg']};
        padding: 1.5rem;
        border-radius: 16px;
        border: 1px solid {THEMES['border']};
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
    }}
    
    div[data-testid="stMetric"]:hover {{
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        transform: translateY(-2px);
    }}
    
    /* Info boxes */
    .stAlert {{
        background: linear-gradient(135deg, {THEMES['gradient_start']} 0%, {THEMES['gradient_end']} 100%);
        color: white !important;
        border: none;
        border-radius: 12px;
        padding: 1rem 1.5rem;
        font-weight: 500;
    }}
    
    /* Dataframe */
    [data-testid="stDataFrame"] {{
        border-radius: 12px;
        border: 1px solid {THEMES['border']};
        overflow: hidden;
    }}
    
    /* Divider */
    hr {{
        margin: 3rem 0;
        border: none;
        height: 1px;
        background: linear-gradient(90deg, 
            rgba(226,232,240,0) 0%, 
            {THEMES['border']} 50%, 
            rgba(226,232,240,0) 100%);
    }}
    
    /* Charts */
    [data-testid="stPlotlyChart"] {{
        background: {THEMES['chart_bg']};
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid {THEMES['border']};
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }}
    
    /* Sidebar styling */
    .sidebar .sidebar-content {{
        background-color: {THEMES['secondary_bg']};
    }}
    
    /* Hide default elements */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown(f"""
    <h1>📊 Hacker News Analytics</h1>
    <p style='color: {THEMES['text_secondary']}; font-size: 1.1rem; margin-top: -0.5rem;'>
        Real-time insights from production data pipeline
    </p>
""", unsafe_allow_html=True)

st.markdown("---")

# Load Data
@st.cache_data(ttl=300)
def get_data():
    engine = create_engine(DB_URL)
    
    stats_df = pd.read_sql(
        "SELECT * FROM daily_stats ORDER BY stat_date DESC LIMIT 30", 
        engine
    )
    
    stories_df = pd.read_sql("""
        SELECT title, score, domain, by as author, posted_at
        FROM stories 
        ORDER BY score DESC 
        LIMIT 20
    """, engine)
    
    type_dist = pd.read_sql("""
        SELECT story_type, COUNT(*) as count, AVG(score) as avg_score
        FROM stories
        GROUP BY story_type
        ORDER BY count DESC
    """, engine)
    
    hour_dist = pd.read_sql("""
        SELECT hour_posted, COUNT(*) as count, AVG(score) as avg_score
        FROM stories
        GROUP BY hour_posted
        ORDER BY hour_posted
    """, engine)
    
    top_domains = pd.read_sql("""
        SELECT domain, COUNT(*) as count, AVG(score) as avg_score
        FROM stories
        WHERE domain IS NOT NULL AND domain != ''
        GROUP BY domain
        ORDER BY count DESC
        LIMIT 15
    """, engine)
    
    total_stats = pd.read_sql("""
        SELECT 
            COUNT(*) as total_stories,
            AVG(score) as avg_score,
            MAX(score) as max_score,
            COUNT(DISTINCT by) as unique_authors
        FROM stories
    """, engine)
    
    return stats_df, stories_df, type_dist, hour_dist, top_domains, total_stats

try:
    df_stats, df_stories, df_types, df_hours, df_domains, total_stats = get_data()
    
    st.caption(f"🔄 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # === KEY METRICS ===
    st.markdown("### 📈 Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    latest = df_stats.iloc[0] if not df_stats.empty else None
    stats = total_stats.iloc[0]
    
    with col1:
        if latest is not None:
            st.metric("Latest Run", str(latest['stat_date']))
    with col2:
        st.metric("Total Stories", f"{int(stats['total_stories']):,}")
    with col3:
        st.metric("Avg Score", f"{stats['avg_score']:.1f}")
    with col4:
        st.metric("Max Score", f"{int(stats['max_score']):,}")
    with col5:
        st.metric("Unique Authors", f"{int(stats['unique_authors']):,}")
    
    st.markdown("---")
    
    # === TRENDS ===
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Story Volume Trend")
        fig_volume = go.Figure()
        fig_volume.add_trace(go.Scatter(
            x=df_stats['stat_date'],
            y=df_stats['total_stories'],
            mode='lines+markers',
            line=dict(color='#667eea', width=3),
            marker=dict(size=8, color='#667eea'),
            fill='tozeroy',
            fillcolor='rgba(102, 126, 234, 0.1)',
            hovertemplate='<b>%{x}</b><br>Stories: %{y:,}<extra></extra>'
        ))
        fig_volume.update_layout(
            title="Stories Collected Per Day",
            xaxis_title="Date",
            yaxis_title="Number of Stories",
            hovermode='x unified',
            plot_bgcolor=THEMES['chart_bg'],
            paper_bgcolor=THEMES['chart_bg'],
            font=dict(family='Inter', color=THEMES['text']),
            margin=dict(t=50, l=60, r=20, b=50),
            height=350
        )
        st.plotly_chart(fig_volume, use_container_width=True)
    
    with col2:
        st.markdown("### 🎯 Average Score Trend")
        fig_score = go.Figure()
        fig_score.add_trace(go.Scatter(
            x=df_stats['stat_date'],
            y=df_stats['avg_score'],
            mode='lines+markers',
            line=dict(color='#f093fb', width=3),
            marker=dict(size=8, color='#f093fb'),
            fill='tozeroy',
            fillcolor='rgba(240, 147, 251, 0.1)',
            hovertemplate='<b>%{x}</b><br>Avg Score: %{y:.1f}<extra></extra>'
        ))
        fig_score.update_layout(
            title="Average Story Score Over Time",
            xaxis_title="Date",
            yaxis_title="Average Score",
            hovermode='x unified',
            plot_bgcolor=THEMES['chart_bg'],
            paper_bgcolor=THEMES['chart_bg'],
            font=dict(family='Inter', color=THEMES['text']),
            margin=dict(t=50, l=60, r=20, b=50),
            height=350
        )
        st.plotly_chart(fig_score, use_container_width=True)
    
    st.markdown("---")
    
    # === DISTRIBUTION ===
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📝 Story Type Distribution")
        fig_types = px.bar(
            df_types,
            x='story_type',
            y='count',
            color='avg_score',
            title="",
            color_continuous_scale=['#667eea', '#764ba2', '#f093fb']
        )
        fig_types.update_layout(
            xaxis_title="Story Type",
            yaxis_title="Count",
            showlegend=False,
            plot_bgcolor=THEMES['chart_bg'],
            paper_bgcolor=THEMES['chart_bg'],
            font=dict(family='Inter', color=THEMES['text']),
            height=350
        )
        st.plotly_chart(fig_types, use_container_width=True)
        
        st.dataframe(
            df_types.rename(columns={
                'story_type': 'Type',
                'count': 'Count',
                'avg_score': 'Avg Score'
            }).style.format({
                'Count': '{:,.0f}',
                'Avg Score': '{:.1f}'
            }),
            use_container_width=True,
            hide_index=True
        )
    
    with col2:
        st.markdown("### 🕐 Best Time to Post")
        fig_hours = px.bar(
            df_hours,
            x='hour_posted',
            y='count',
            color='avg_score',
            title="",
            color_continuous_scale=['#4facfe', '#00f2fe']
        )
        fig_hours.update_layout(
            xaxis_title="Hour of Day (UTC)",
            yaxis_title="Number of Posts",
            xaxis=dict(tickmode='linear', tick0=0, dtick=2),
            plot_bgcolor=THEMES['chart_bg'],
            paper_bgcolor=THEMES['chart_bg'],
            font=dict(family='Inter', color=THEMES['text']),
            height=350
        )
        st.plotly_chart(fig_hours, use_container_width=True)
        
        best_hour = df_hours.loc[df_hours['avg_score'].idxmax()]
        st.info(f"💡 Best time to post: {int(best_hour['hour_posted'])}:00 UTC (Avg score: {best_hour['avg_score']:.1f})")
    
    st.markdown("---")
    
    # === TOP DOMAINS ===
    st.markdown("### 🌐 Top Domains")
    fig_domains = px.bar(
        df_domains,
        x='count',
        y='domain',
        orientation='h',
        color='avg_score',
        title="",
        color_continuous_scale=['#a8edea', '#fed6e3']
    )
    fig_domains.update_layout(
        xaxis_title="Number of Stories",
        yaxis_title="",
        height=500,
        plot_bgcolor=THEMES['chart_bg'],
        paper_bgcolor=THEMES['chart_bg'],
        font=dict(family='Inter', color=THEMES['text'])
    )
    st.plotly_chart(fig_domains, use_container_width=True)
    
    st.markdown("---")
    
    # === TOP STORIES ===
    st.markdown("### 🔥 Top Stories")
    
    df_stories['rank'] = range(1, len(df_stories) + 1)
    display_df = df_stories[['rank', 'title', 'score', 'author', 'domain']].copy()
    display_df.columns = ['#', 'Title', 'Score', 'Author', 'Domain']
    
    st.dataframe(
        display_df.style.format({'Score': '{:,.0f}'}),
        use_container_width=True,
        hide_index=True,
        height=600
    )
    
    # === FOOTER ===
    st.markdown("---")
    st.caption("📊 Pipeline: Hacker News API → Airflow → PySpark → PostgreSQL")
    st.caption("🔄 Updates every 5 minutes | Built with Streamlit")

except Exception as e:
    st.error("⚠️ Unable to connect to database")
    st.info("Make sure your pipeline has run at least once and PostgreSQL is running on port 5435")
    with st.expander("Error Details"):
        st.code(str(e))