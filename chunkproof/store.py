import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS delivery(id TEXT PRIMARY KEY,status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS manifest(version TEXT PRIMARY KEY,delivery_id TEXT NOT NULL REFERENCES delivery(id),body_hash TEXT NOT NULL,body BLOB NOT NULL,created TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS file_entry(id INTEGER PRIMARY KEY,version TEXT NOT NULL REFERENCES manifest(version),path TEXT NOT NULL,size INTEGER NOT NULL,sha256 TEXT NOT NULL,UNIQUE(version,path));
CREATE TABLE IF NOT EXISTS chunk(id INTEGER PRIMARY KEY,file_id INTEGER NOT NULL REFERENCES file_entry(id),idx INTEGER NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,sha256 TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'planned',UNIQUE(file_id,idx));
CREATE TABLE IF NOT EXISTS attempt(id INTEGER PRIMARY KEY,version TEXT NOT NULL REFERENCES manifest(version),medium TEXT NOT NULL,source_hash TEXT NOT NULL,source BLOB NOT NULL,root TEXT NOT NULL,created TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(version,medium,source_hash));
CREATE TABLE IF NOT EXISTS receipt(id INTEGER PRIMARY KEY,attempt_id INTEGER NOT NULL REFERENCES attempt(id),chunk_id INTEGER REFERENCES chunk(id),medium TEXT NOT NULL,path TEXT NOT NULL,claimed_hash TEXT,actual_hash TEXT,actual_size INTEGER,status TEXT NOT NULL,UNIQUE(attempt_id,medium,path));
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY,version TEXT NOT NULL,code TEXT NOT NULL,detail TEXT NOT NULL,receipt_id INTEGER REFERENCES receipt(id));
CREATE TABLE IF NOT EXISTS decision(version TEXT PRIMARY KEY,result TEXT NOT NULL,detail TEXT NOT NULL,created TEXT DEFAULT CURRENT_TIMESTAMP);
"""
def connect(path):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path); db.row_factory=sqlite3.Row; db.executescript(SCHEMA); return db
