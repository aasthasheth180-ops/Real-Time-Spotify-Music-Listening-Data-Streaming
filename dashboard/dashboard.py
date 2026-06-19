"""
Streamlit dashboard — connects to PostgreSQL and shows your listening stats.
Run with:  streamlit run dashboard/dashboard.py
"""

import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Spotify Dashboard", page_icon="🎵", layout="wide")

# ── Dark theme styling ────────────────────────────────────────
st.markdown("""
<style>
  [data-testid="stAppViewContainer"] { background:#0D0D0D; }
  h1, h2, h3 { color:#1DB954; }
  .metric-card {
    background:#1A1A1A; border-radius:12px;
    padding:1rem 1.2rem; border:1px solid #1DB954;
    text-align:center; margin-bottom:1rem;
  }
</style>
""", unsafe_allow_html=True)

st.title("🎵 My Spotify Listening Dashboard")
st.caption("Real-time data from your Kafka + PySpark pipeline")

# ── Connect to PostgreSQL ─────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(os.getenv("DATABASE_URL"))

engine = get_engine()


@st.cache_data(ttl=60)   # refresh every 60 seconds
def load_data():
    with engine.connect() as conn:
        # Top songs this week
        top_songs = pd.read_sql(text("""
            SELECT s.song_name, COUNT(*) as plays
            FROM fact_history f
            JOIN dim_song s ON f.song_id = s.song_id
            WHERE f.played_at >= NOW() - INTERVAL '7 days'
            GROUP BY s.song_name ORDER BY plays DESC LIMIT 10
        """), conn)

        # Top artists this week
        top_artists = pd.read_sql(text("""
            SELECT a.artist_name, COUNT(*) as plays
            FROM fact_history f
            JOIN dim_artist a ON f.artist_id = a.artist_id
            WHERE f.played_at >= NOW() - INTERVAL '7 days'
            GROUP BY a.artist_name ORDER BY plays DESC LIMIT 10
        """), conn)

        # Listening by day of week
        by_day = pd.read_sql(text("""
            SELECT TO_CHAR(played_at, 'Day') as day_name,
                   EXTRACT(DOW FROM played_at) as day_num,
                   COUNT(*) as plays
            FROM fact_history
            WHERE played_at >= NOW() - INTERVAL '30 days'
            GROUP BY day_name, day_num
            ORDER BY day_num
        """), conn)

        # Total stats
        totals = pd.read_sql(text("""
            SELECT
              COUNT(*) as total_plays,
              COUNT(DISTINCT song_id) as unique_songs,
              COUNT(DISTINCT artist_id) as unique_artists,
              COALESCE(SUM(duration_ms)/60000, 0) as total_minutes
            FROM fact_history
            WHERE played_at >= NOW() - INTERVAL '7 days'
        """), conn)

    return top_songs, top_artists, by_day, totals


try:
    top_songs, top_artists, by_day, totals = load_data()

    # ── KPI Row ───────────────────────────────────────────────
    t = totals.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🎵 Total Plays",      int(t["total_plays"]))
    c2.metric("🎶 Unique Songs",     int(t["unique_songs"]))
    c3.metric("🎤 Unique Artists",   int(t["unique_artists"]))
    c4.metric("⏱ Minutes Listened",  int(t["total_minutes"]))

    st.divider()

    # ── Charts ────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🎵 Top 10 Songs This Week")
        if not top_songs.empty:
            st.bar_chart(top_songs.set_index("song_name")["plays"])
        else:
            st.info("No data yet — play some music and wait 30 seconds!")

    with col2:
        st.subheader("🎤 Top 10 Artists This Week")
        if not top_artists.empty:
            st.bar_chart(top_artists.set_index("artist_name")["plays"])
        else:
            st.info("No data yet!")

    st.subheader("📅 Listening by Day of Week (last 30 days)")
    if not by_day.empty:
        st.bar_chart(by_day.set_index("day_name")["plays"])

    # ── Raw data table ────────────────────────────────────────
    st.divider()
    st.subheader("📋 Recent Plays")
    with engine.connect() as conn:
        recent = pd.read_sql(text("""
            SELECT f.played_at, s.song_name, a.artist_name, al.album_name
            FROM fact_history f
            JOIN dim_song   s  ON f.song_id   = s.song_id
            JOIN dim_artist a  ON f.artist_id  = a.artist_id
            JOIN dim_album  al ON f.album_id   = al.album_id
            ORDER BY f.played_at DESC
            LIMIT 20
        """), conn)
    st.dataframe(recent, hide_index=True, use_container_width=True)

    st.caption("Auto-refreshes every 60 seconds · Data from Kafka → PySpark → PostgreSQL")

except Exception as e:
    st.error(f"Database error: {e}")
    st.info("Make sure PostgreSQL is running and you have data in fact_history table")