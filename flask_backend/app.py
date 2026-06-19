"""
Flask backend — handles Spotify OAuth and triggers Weekly Wrapped.
Run with:  flask run --reload
"""

import os
from flask import Flask, redirect, request, jsonify, session
from dotenv import load_dotenv
from flask_backend.utils import (
    get_auth_url, exchange_code_for_token, load_token,
    get_recently_played, build_weekly_stats,
    generate_gemini_insight, send_weekly_email
)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)


@app.route("/")
def index():
    return jsonify({"status": "Spotify Pipeline running", "endpoints": [
        "GET  /login    → start Spotify OAuth",
        "GET  /callback → OAuth callback (handled automatically)",
        "GET  /recent   → show last 50 played tracks",
        "GET  /insight  → generate + send Weekly Wrapped email",
    ]})


@app.route("/login")
def login():
    return redirect(get_auth_url())


@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return jsonify({"error": "No code returned from Spotify"}), 400
    token_info = exchange_code_for_token(code)
    return jsonify({
        "status" : "✅ Authenticated with Spotify!",
        "expires": token_info.get("expires_at"),
        "next"   : "Visit /recent to see your tracks or /insight for Weekly Wrapped"
    })


@app.route("/recent")
def recent():
    token_info = load_token()
    if not token_info:
        return redirect("/login")
    tracks = get_recently_played(token_info)
    return jsonify({"count": len(tracks), "tracks": [
        {"name": t["track"]["name"],
         "artist": t["track"]["artists"][0]["name"],
         "played_at": t["played_at"]}
        for t in tracks
    ]})


@app.route("/insight")
def insight():
    """Build stats → Gemini → email."""
    try:
        stats   = build_weekly_stats()
        text    = generate_gemini_insight(stats)
        send_weekly_email(text, stats)
        return jsonify({
            "status"        : "✅ Weekly Wrapped email sent!",
            "total_minutes" : stats["total_minutes"],
            "top_song"      : stats["top_songs"][0]["name"] if stats["top_songs"] else "none",
            "preview"       : text[:200] + "..."
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)