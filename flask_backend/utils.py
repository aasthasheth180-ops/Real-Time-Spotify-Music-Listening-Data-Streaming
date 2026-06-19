"""
Spotify OAuth helpers and Gemini AI Weekly Wrapped email sender.
"""

import os
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

import requests
from google import genai
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ── Spotify OAuth ─────────────────────────────────────────────
SPOTIFY_AUTH_URL    = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL   = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE    = "https://api.spotify.com/v1"
SPOTIFY_SCOPE       = "user-read-recently-played user-top-read"

CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("SPOTIFY_REDIRECT_URI")
TOKEN_FILE    = "flask_backend/token_info.json"


def get_auth_url() -> str:
    params = {
        "client_id"    : CLIENT_ID,
        "response_type": "code",
        "redirect_uri" : REDIRECT_URI,
        "scope"        : SPOTIFY_SCOPE,
    }
    req = requests.Request("GET", SPOTIFY_AUTH_URL, params=params).prepare()
    return req.url


def exchange_code_for_token(code: str) -> dict:
    resp = requests.post(SPOTIFY_TOKEN_URL, data={
        "grant_type"  : "authorization_code",
        "code"        : code,
        "redirect_uri": REDIRECT_URI,
    }, headers={
        "Authorization": requests.auth._basic_auth_str(CLIENT_ID, CLIENT_SECRET),
        "Content-Type" : "application/x-www-form-urlencoded",
    })
    token_info = resp.json()
    token_info["expires_at"] = (
        datetime.now().timestamp() + token_info.get("expires_in", 3600)
    )
    with open(TOKEN_FILE, "w") as f:
        json.dump(token_info, f)
    return token_info


def load_token() -> dict | None:
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE) as f:
        return json.load(f)


def refresh_token_if_needed(token_info: dict) -> dict:
    if datetime.now().timestamp() > token_info.get("expires_at", 0) - 60:
        resp = requests.post(SPOTIFY_TOKEN_URL, data={
            "grant_type"   : "refresh_token",
            "refresh_token": token_info["refresh_token"],
        }, headers={
            "Authorization": requests.auth._basic_auth_str(CLIENT_ID, CLIENT_SECRET),
            "Content-Type" : "application/x-www-form-urlencoded",
        })
        new_info = resp.json()
        new_info["refresh_token"] = token_info["refresh_token"]
        new_info["expires_at"]    = datetime.now().timestamp() + new_info.get("expires_in", 3600)
        with open(TOKEN_FILE, "w") as f:
            json.dump(new_info, f)
        return new_info
    return token_info


def get_recently_played(token_info: dict, limit: int = 50) -> list:
    token_info = refresh_token_if_needed(token_info)

    headers = {
        "Authorization": f"Bearer {token_info['access_token']}"
    }

    resp = requests.get(
        f"{SPOTIFY_API_BASE}/me/player/recently-played",
        headers=headers,
        params={"limit": limit}
    )

    print("Status Code:", resp.status_code)
    print("Response:", resp.text)

    # Check if response is successful
    if resp.status_code != 200:
        raise Exception(f"Spotify API Error: {resp.status_code} - {resp.text}")

    # Safe JSON parsing
    try:
        data = resp.json()
    except Exception:
        raise Exception("Invalid JSON response from Spotify API")

    return data.get("items", [])


# ── Weekly Wrapped via Gemini ─────────────────────────────────
def build_weekly_stats() -> dict:
    """Query PostgreSQL for the past 7 days of listening history."""
    engine = create_engine(os.getenv("DATABASE_URL"))
    with engine.connect() as conn:
        # FIX: PostgreSQL date syntax (not MySQL DATE_SUB/CURDATE)
        top_songs = conn.execute(text("""
            SELECT s.song_name, COUNT(*) as plays
            FROM fact_history f
            JOIN dim_song s ON f.song_id = s.song_id
            WHERE f.played_at >= NOW() - INTERVAL '7 days'
            GROUP BY s.song_name
            ORDER BY plays DESC
            LIMIT 5
        """)).fetchall()

        top_artists = conn.execute(text("""
            SELECT a.artist_name, COUNT(*) as plays
            FROM fact_history f
            JOIN dim_artist a ON f.artist_id = a.artist_id
            WHERE f.played_at >= NOW() - INTERVAL '7 days'
            GROUP BY a.artist_name
            ORDER BY plays DESC
            LIMIT 5
        """)).fetchall()

        total_minutes = conn.execute(text("""
            SELECT COALESCE(SUM(f.duration_ms) / 60000, 0) as mins
            FROM fact_history f
            WHERE f.played_at >= NOW() - INTERVAL '7 days'
        """)).scalar()

    return {
        "top_songs"     : [{"name": r[0], "plays": r[1]} for r in top_songs],
        "top_artists"   : [{"name": r[0], "plays": r[1]} for r in top_artists],
        "total_minutes" : int(total_minutes or 0),
    }


def generate_gemini_insight(stats: dict) -> str:
    """Send stats to Gemini and get a personalized Weekly Wrapped paragraph."""
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    response = client.models.generate_content(
    model="gemini-1.5-flash",
    contents=prompt
)

    prompt = f"""
You are a friendly music analyst writing a Weekly Wrapped email for a Spotify user.

Their listening stats for the past 7 days:
- Total listening time: {stats['total_minutes']} minutes
- Top 5 songs: {[s['name'] + ' (' + str(s['plays']) + ' plays)' for s in stats['top_songs']]}
- Top 5 artists: {[a['name'] + ' (' + str(a['plays']) + ' plays)' for a in stats['top_artists']]}

Write a warm, fun 3-paragraph email summary. Include:
1. An opening that highlights their total listening time with an enthusiastic tone
2. A paragraph about their top songs and what the pattern reveals about their mood/taste
3. A closing paragraph about their top artists and a fun observation or music recommendation

Keep it personal, upbeat, and under 200 words. No bullet points — prose only.
"""
    return response.text


def send_weekly_email(insight_text: str, stats: dict):
    """Send the Gemini-generated insight as an HTML email."""
    sender   = os.getenv("SENDER_EMAIL")
    password = os.getenv("SENDER_PASSWORD")
    receiver = os.getenv("RECEIVER_EMAIL")

    html_body = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:600px;margin:auto;padding:20px;">
      <h1 style="color:#1DB954;">🎵 Your Weekly Wrapped</h1>
      <p style="color:#333;line-height:1.7;">{insight_text.replace(chr(10), '<br>')}</p>
      <hr style="border:1px solid #eee;margin:20px 0;">
      <h3 style="color:#1DB954;">📊 By the Numbers</h3>
      <p><strong>Total listening time:</strong> {stats['total_minutes']} minutes</p>
      <h4>🎵 Top Songs</h4>
      <ol>{''.join(f"<li>{s['name']} — {s['plays']} plays</li>" for s in stats['top_songs'])}</ol>
      <h4>🎤 Top Artists</h4>
      <ol>{''.join(f"<li>{a['name']} — {a['plays']} plays</li>" for a in stats['top_artists'])}</ol>
      <p style="color:#999;font-size:12px;margin-top:30px;">Generated by your Spotify Pipeline · Powered by Gemini AI</p>
    </body></html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "🎵 Your Weekly Spotify Wrapped"
    msg["From"]    = sender
    msg["To"]      = receiver
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, password)
        server.sendmail(sender, receiver, msg.as_string())

    print("✅ Weekly Wrapped email sent!")