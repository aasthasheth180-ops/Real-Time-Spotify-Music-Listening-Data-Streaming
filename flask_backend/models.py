"""
PostgreSQL Star Schema for Spotify pipeline.
Run this ONCE to create all tables:  python models.py
"""

from sqlalchemy import (
    create_engine, Column, Integer, String,
    Float, DateTime, UniqueConstraint
)
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
import os

load_dotenv()

Base = declarative_base()
engine = create_engine(os.getenv("DATABASE_URL"))


class DimSong(Base):
    __tablename__ = "dim_song"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    song_id   = Column(String(100), unique=True, nullable=False)
    song_name = Column(String(300))
    duration  = Column(Integer)     # milliseconds
    explicit  = Column(String(10))
    popularity= Column(Integer)


class DimAlbum(Base):
    __tablename__ = "dim_album"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    album_id        = Column(String(100), unique=True, nullable=False)
    album_name      = Column(String(300))
    album_type      = Column(String(50))
    release_date    = Column(String(20))
    total_tracks    = Column(Integer)


class DimArtist(Base):
    __tablename__ = "dim_artist"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    artist_id   = Column(String(100), unique=True, nullable=False)
    artist_name = Column(String(300))
    external_url= Column(String(500))


class FactHistory(Base):
    __tablename__ = "fact_history"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    played_at   = Column(DateTime, nullable=False)
    song_id     = Column(String(100))
    album_id    = Column(String(100))
    artist_id   = Column(String(100))
    duration_ms = Column(Integer)

    # FIX: deduplication constraint — prevents double inserts on pipeline restart
    __table_args__ = (
        UniqueConstraint("played_at", "song_id", name="uniq_play"),
    )


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    print("✅ All tables created in PostgreSQL")