import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from core.models import MediaAsset, Message

_MEDIA_PATH_SEP = "\x1f"

class DatabaseManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;") 
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS app_config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assets (
                    asset_id TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    file_type TEXT,
                    file_size INTEGER,
                    timestamp DATETIME,
                    checksum TEXT,
                    overlay_path TEXT,
                    is_favorite INTEGER NOT NULL DEFAULT 0
                )
            """)
            
            try:
                cursor.execute("SELECT overlay_path FROM assets LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE assets ADD COLUMN overlay_path TEXT")
            try:
                cursor.execute("SELECT is_favorite FROM assets LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE assets ADD COLUMN is_favorite INTEGER NOT NULL DEFAULT 0")

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_timestamp ON assets(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_path ON assets(file_path)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assets_favorite_time ON assets(is_favorite, timestamp)")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    username TEXT PRIMARY KEY,
                    display_name TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    content TEXT,
                    timestamp DATETIME NOT NULL,
                    msg_type TEXT,
                    source TEXT,
                    FOREIGN KEY (username) REFERENCES conversations (username)
                )
            """)
            try:
                cursor.execute("SELECT source FROM messages LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE messages ADD COLUMN source TEXT")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS message_media (
                    message_id INTEGER,
                    asset_id TEXT,
                    FOREIGN KEY (message_id) REFERENCES messages (id),
                    FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_user_time ON messages(username, timestamp, id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(username, id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_message_media_msg ON message_media(message_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_message_media_asset ON message_media(asset_id)")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    color TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS asset_tags (
                    asset_id TEXT NOT NULL,
                    tag_id INTEGER NOT NULL,
                    PRIMARY KEY (asset_id, tag_id),
                    FOREIGN KEY (asset_id) REFERENCES assets (asset_id) ON DELETE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_asset_tags_asset ON asset_tags(asset_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_asset_tags_tag ON asset_tags(tag_id)")
            conn.commit()
    
    def set_config(self, key: str, value: str):
        with self._connect() as conn:
            conn.execute("INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)", (key, value))

    def get_config(self, key: str) -> str | None:
        with self._connect() as conn:
            cursor = conn.execute("SELECT value FROM app_config WHERE key = ?", (key,))
            res = cursor.fetchone()
            return res[0] if res else None

    def add_asset(self, asset: MediaAsset):
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO assets (asset_id, file_path, file_type, file_size, timestamp, overlay_path)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id) DO UPDATE SET
                    file_path=excluded.file_path,
                    file_type=excluded.file_type,
                    file_size=excluded.file_size,
                    timestamp=excluded.timestamp,
                    overlay_path=excluded.overlay_path
            """, (asset.asset_id, asset.file_path, asset.file_type, asset.file_size, asset.created_at, asset.overlay_path))

    def add_assets_batch(self, assets: list[MediaAsset]):
        if not assets: return
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            conn.executemany("""
                INSERT INTO assets (asset_id, file_path, file_type, file_size, timestamp, overlay_path)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id) DO UPDATE SET
                    file_path=excluded.file_path,
                    file_type=excluded.file_type,
                    file_size=excluded.file_size,
                    timestamp=excluded.timestamp,
                    overlay_path=excluded.overlay_path
            """, [(a.asset_id, a.file_path, a.file_type, a.file_size, a.created_at, a.overlay_path) for a in assets])
            conn.commit()

    def find_media_by_time(self, timestamp, fuzziness=2):
        if not timestamp: return []
        
        with self._connect() as conn:
            minute_prefix = timestamp.strftime("%Y-%m-%d %H:%M")
            cursor = conn.execute("""
                SELECT file_path FROM assets 
                WHERE timestamp LIKE ? || '%'
                AND file_path NOT LIKE '%overlay%' 
                AND file_path NOT LIKE '%thumbnail%'
                AND file_path NOT LIKE '%caption%'
            """, (minute_prefix,))
            return [row[0] for row in cursor.fetchall()]

    def update_conversation_title(self, username: str, title: str):
        with self._connect() as conn:
            conn.execute("INSERT OR IGNORE INTO conversations (username) VALUES (?)", (username,))
            conn.execute("UPDATE conversations SET display_name = ? WHERE username = ?", (title, username))

    def add_message(self, username: str, msg: Message):
        with self._connect() as conn:
            conn.execute("INSERT OR IGNORE INTO conversations (username) VALUES (?)", (username,))
            cursor = conn.execute("""
                INSERT INTO messages (username, sender, content, timestamp, msg_type, source)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (username, msg.sender, msg.content, msg.timestamp, msg.msg_type, getattr(msg, "source", None)))
            
            msg_id = cursor.lastrowid
            if msg.media_refs:
                refs = [ref for ref in msg.media_refs if ref]
                if refs:
                    placeholders = ",".join("?" for _ in refs)
                    rows = conn.execute(
                        f"SELECT asset_id, file_path FROM assets WHERE file_path IN ({placeholders})",
                        refs,
                    ).fetchall()
                    asset_map = {row[1]: row[0] for row in rows}
                    pairs = [(msg_id, asset_map[ref]) for ref in dict.fromkeys(refs) if ref in asset_map]
                    if pairs:
                        conn.executemany(
                            "INSERT INTO message_media (message_id, asset_id) VALUES (?, ?)",
                            pairs,
                        )

    def clear_messages(self):
        with self._connect() as conn:
            conn.execute("DELETE FROM message_media")
            conn.execute("DELETE FROM messages")

    def clear_messages_for_conversations(self, usernames: list[str]):
        usernames = [username for username in usernames if username]
        if not usernames:
            return

        placeholders = ",".join("?" for _ in usernames)
        with self._connect() as conn:
            conn.execute(
                f"""
                DELETE FROM message_media
                WHERE message_id IN (
                    SELECT id
                    FROM messages
                    WHERE username IN ({placeholders})
                )
                """,
                usernames,
            )
            conn.execute(
                f"DELETE FROM messages WHERE username IN ({placeholders})",
                usernames,
            )

    def get_conversations(self):
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT username, display_name FROM conversations")
            return [dict(row) for row in cursor.fetchall()]

    def get_messages(self, username: str, limit: int = 50):
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT m.id, m.sender, m.content, m.timestamp, m.msg_type, m.source,
                       group_concat(a.file_path, ?) as media_paths
                FROM messages m
                LEFT JOIN message_media mm ON m.id = mm.message_id
                LEFT JOIN assets a ON mm.asset_id = a.asset_id
                WHERE m.username = ? 
                GROUP BY m.id
                ORDER BY m.timestamp DESC, m.id DESC
                LIMIT ?
            """, (_MEDIA_PATH_SEP, username, limit))
            
            results = []
            for row in cursor.fetchall():
                media = row[6].split(_MEDIA_PATH_SEP) if row[6] else []
                results.append(Message(
                    id=row[0],
                    sender=row[1],
                    content=row[2],
                    timestamp=row[3],
                    msg_type=row[4],
                    source=row[5] or "chat",
                    media_refs=media
                ))
            return results

    def get_messages_paginated(self, username: str, limit: int = 50, offset: int = 0):
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT m.id, m.sender, m.content, m.timestamp, m.msg_type, m.source,
                       group_concat(a.file_path, ?) as media_paths
                FROM messages m
                LEFT JOIN message_media mm ON m.id = mm.message_id
                LEFT JOIN assets a ON mm.asset_id = a.asset_id
                WHERE m.username = ?
                GROUP BY m.id
                ORDER BY m.timestamp DESC, m.id DESC
                LIMIT ? OFFSET ?
                """,
                (_MEDIA_PATH_SEP, username, limit, offset),
            )

            results = []
            for row in cursor.fetchall():
                media = row[6].split(_MEDIA_PATH_SEP) if row[6] else []
                results.append(
                    Message(
                        id=row[0],
                        sender=row[1],
                        content=row[2],
                        timestamp=row[3],
                        msg_type=row[4],
                        source=row[5] or "chat",
                        media_refs=media,
                    )
                )
            return results

    def count_messages_for_conversation(self, username: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE username = ?",
                (username,),
            ).fetchone()
            return int(row[0]) if row else 0

    def get_messages_before(self, username: str, before_ts, before_id: int, limit: int = 50):
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT m.id, m.sender, m.content, m.timestamp, m.msg_type, m.source,
                       group_concat(a.file_path, ?) as media_paths
                FROM messages m
                LEFT JOIN message_media mm ON m.id = mm.message_id
                LEFT JOIN assets a ON mm.asset_id = a.asset_id
                WHERE m.username = ?
                AND (m.timestamp < ? OR (m.timestamp = ? AND m.id < ?))
                GROUP BY m.id
                ORDER BY m.timestamp DESC, m.id DESC
                LIMIT ?
            """, (_MEDIA_PATH_SEP, username, before_ts, before_ts, before_id, limit))

            results = []
            for row in cursor.fetchall():
                media = row[6].split(_MEDIA_PATH_SEP) if row[6] else []
                results.append(Message(
                    id=row[0],
                    sender=row[1],
                    content=row[2],
                    timestamp=row[3],
                    msg_type=row[4],
                    source=row[5] or "chat",
                    media_refs=media
                ))
            return results

    def get_messages_after(self, username: str, after_ts, after_id: int, limit: int = 50):
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT m.id, m.sender, m.content, m.timestamp, m.msg_type, m.source,
                       group_concat(a.file_path, ?) as media_paths
                FROM messages m
                LEFT JOIN message_media mm ON m.id = mm.message_id
                LEFT JOIN assets a ON mm.asset_id = a.asset_id
                WHERE m.username = ?
                AND (m.timestamp > ? OR (m.timestamp = ? AND m.id > ?))
                GROUP BY m.id
                ORDER BY m.timestamp ASC, m.id ASC
                LIMIT ?
            """, (_MEDIA_PATH_SEP, username, after_ts, after_ts, after_id, limit))

            results = []
            for row in cursor.fetchall():
                media = row[6].split(_MEDIA_PATH_SEP) if row[6] else []
                results.append(Message(
                    id=row[0],
                    sender=row[1],
                    content=row[2],
                    timestamp=row[3],
                    msg_type=row[4],
                    source=row[5] or "chat",
                    media_refs=media
                ))
            return results

    def get_message_index(self, username: str):
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT m.id, m.sender, m.timestamp, m.content, COUNT(mm.asset_id) as media_count
                FROM messages m
                LEFT JOIN message_media mm ON m.id = mm.message_id
                WHERE m.username = ?
                GROUP BY m.id
                ORDER BY m.timestamp ASC, m.id ASC
            """, (username,))
            results = []
            for row in cursor.fetchall():
                msg_id, sender, ts, content, media_count = row
                if not content and (media_count or 0) == 0:
                    continue
                ts_val = ts
                if isinstance(ts, str):
                    try:
                        ts_val = datetime.fromisoformat(ts)
                    except Exception:
                        ts_val = None
                results.append({
                    "id": msg_id,
                    "sender": sender,
                    "timestamp": ts_val
                })
            return results

    def get_messages_by_ids(self, ids: list[int]):
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        with self._connect() as conn:
            cursor = conn.execute(f"""
                SELECT m.id, m.sender, m.content, m.timestamp, m.msg_type, m.source,
                       group_concat(a.file_path, ?) as media_paths
                FROM messages m
                LEFT JOIN message_media mm ON m.id = mm.message_id
                LEFT JOIN assets a ON mm.asset_id = a.asset_id
                WHERE m.id IN ({placeholders})
                GROUP BY m.id
            """, (_MEDIA_PATH_SEP, *ids))
            results = []
            for row in cursor.fetchall():
                media = row[6].split(_MEDIA_PATH_SEP) if row[6] else []
                results.append(Message(
                    id=row[0],
                    sender=row[1],
                    content=row[2],
                    timestamp=row[3],
                    msg_type=row[4],
                    source=row[5] or "chat",
                    media_refs=media
                ))
            return results

    def get_conversation_senders(self) -> dict[str, set[str]]:
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT m.username, m.sender
                FROM messages m
                GROUP BY m.username, m.sender
            """)
            results: dict[str, set[str]] = {}
            for row in cursor.fetchall():
                username, sender = row
                if username is None or sender is None:
                    continue
                results.setdefault(username, set()).add(sender)
            return results

    # --- UPDATED QUERY METHODS ---

    def get_memory_years(self):
        """Returns a list of years available in memories."""
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT DISTINCT strftime('%Y', timestamp) as year 
                FROM assets 
                WHERE file_path NOT LIKE '%chat_media%' 
                AND year IS NOT NULL
                ORDER BY year DESC
            """)
            return [row[0] for row in cursor.fetchall()]

    def _build_date_filter(self, year, month, day):
        query_part = ""
        params = []
        
        # LOGIC UPDATE: Checks are now independent (not nested)
        # This allows "Year + Day" or "Month + Day" combinations
        
        if year and year != "Year":
            query_part += " AND strftime('%Y', timestamp) = ?"
            params.append(year)
            
        if month and month != "Month":
            query_part += " AND strftime('%m', timestamp) = ?"
            params.append(month)
            
        if day and day != "Day":
            query_part += " AND strftime('%d', timestamp) = ?"
            params.append(day)
                    
        return query_part, params

    def _build_asset_filter(self, year=None, month=None, day=None, favorites_only=False, tags=None, tags_match_all=False):
        date_clause, params = self._build_date_filter(year, month, day)
        clause = " WHERE file_path NOT LIKE '%chat_media%'" + date_clause

        if favorites_only:
            clause += " AND is_favorite = 1"

        if tags:
            tag_placeholders = ",".join("?" for _ in tags)
            if tags_match_all:
                clause += f"""
                    AND asset_id IN (
                        SELECT at.asset_id
                        FROM asset_tags at
                        JOIN tags t ON t.id = at.tag_id
                        WHERE t.name IN ({tag_placeholders})
                        GROUP BY at.asset_id
                        HAVING COUNT(DISTINCT t.name) = ?
                    )
                """
                params.extend(tags)
                params.append(len(tags))
            else:
                clause += f"""
                    AND asset_id IN (
                        SELECT at.asset_id
                        FROM asset_tags at
                        JOIN tags t ON t.id = at.tag_id
                        WHERE t.name IN ({tag_placeholders})
                    )
                """
                params.extend(tags)

        return clause, params

    def count_assets(self, year=None, month=None, day=None, favorites_only=False, tags=None, tags_match_all=False):
        clause, params = self._build_asset_filter(
            year=year,
            month=month,
            day=day,
            favorites_only=favorites_only,
            tags=tags,
            tags_match_all=tags_match_all,
        )

        with self._connect() as conn:
            cursor = conn.execute(f"""
                SELECT COUNT(*) FROM assets 
                {clause}
            """, params)
            res = cursor.fetchone()
            return res[0] if res else 0

    def get_archive_stats(self) -> dict[str, int]:
        with self._connect() as conn:
            memories_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM assets
                WHERE file_path NOT LIKE '%chat_media%'
                """
            ).fetchone()[0]
            chats_count = conn.execute(
                "SELECT COUNT(*) FROM conversations"
            ).fetchone()[0]
            messages_count = conn.execute(
                "SELECT COUNT(*) FROM messages"
            ).fetchone()[0]
            users_count = conn.execute(
                "SELECT COUNT(DISTINCT sender) FROM messages"
            ).fetchone()[0]

        return {
            "memories_count": int(memories_count or 0),
            "chats_count": int(chats_count or 0),
            "messages_count": int(messages_count or 0),
            "users_count": int(users_count or 0),
        }

    def get_assets(
        self,
        limit: int = 50,
        offset: int = 0,
        year=None,
        month=None,
        day=None,
        favorites_only=False,
        tags=None,
        tags_match_all=False,
        sort: str | None = None,
    ):
        clause, params = self._build_asset_filter(
            year=year,
            month=month,
            day=day,
            favorites_only=favorites_only,
            tags=tags,
            tags_match_all=tags_match_all,
        )

        if sort == "date_asc":
            order_by = "timestamp ASC"
        elif sort == "favorites_first":
            order_by = "is_favorite DESC, timestamp DESC"
        else:
            order_by = "timestamp DESC"

        with self._connect() as conn:
            params.extend([limit, offset])
            cursor = conn.execute(f"""
                SELECT asset_id, file_path, file_type, overlay_path, strftime('%Y', timestamp) as year, is_favorite
                FROM assets 
                {clause}
                ORDER BY {order_by}
                LIMIT ? OFFSET ?
            """, params)
            return cursor.fetchall()

    def set_favorite(self, asset_id: str, is_favorite: bool) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE assets SET is_favorite = ? WHERE asset_id = ?",
                (1 if is_favorite else 0, asset_id),
            )

    def get_favorite(self, asset_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT is_favorite FROM assets WHERE asset_id = ?",
                (asset_id,),
            ).fetchone()
            return bool(row[0]) if row else False

    def get_tags(self) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT id, name, color FROM tags ORDER BY name COLLATE NOCASE"
            )
            return [dict(row) for row in cursor.fetchall()]

    def upsert_tag(self, name: str, color: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM tags WHERE name = ?",
                (name,),
            ).fetchone()
            if row:
                tag_id = int(row[0])
                conn.execute("UPDATE tags SET color = ? WHERE id = ?", (color, tag_id))
                return tag_id
            cursor = conn.execute(
                "INSERT INTO tags (name, color) VALUES (?, ?)",
                (name, color),
            )
            return int(cursor.lastrowid)

    def update_tag_color(self, tag_id: int, color: str) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE tags SET color = ? WHERE id = ?", (color, tag_id))

    def delete_tag(self, tag_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM tags WHERE id = ?", (tag_id,))

    def set_tags_for_asset(self, asset_id: str, tag_names: list[str]) -> None:
        names = [name.strip() for name in tag_names if name and name.strip()]
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            tag_ids: list[int] = []
            for name in names:
                row = conn.execute(
                    "SELECT id FROM tags WHERE name = ?",
                    (name,),
                ).fetchone()
                if row:
                    tag_ids.append(int(row[0]))
                else:
                    cursor = conn.execute(
                        "INSERT INTO tags (name, color) VALUES (?, ?)",
                        (name, "#9B9B9B"),
                    )
                    tag_ids.append(int(cursor.lastrowid))
            conn.execute("DELETE FROM asset_tags WHERE asset_id = ?", (asset_id,))
            if tag_ids:
                conn.executemany(
                    "INSERT OR IGNORE INTO asset_tags (asset_id, tag_id) VALUES (?, ?)",
                    [(asset_id, tag_id) for tag_id in tag_ids],
                )
            conn.commit()

    def get_tags_for_assets(self, asset_ids: list[str]) -> dict[str, list[dict]]:
        if not asset_ids:
            return {}
        placeholders = ",".join("?" for _ in asset_ids)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                SELECT at.asset_id, t.name, t.color
                FROM asset_tags at
                JOIN tags t ON t.id = at.tag_id
                WHERE at.asset_id IN ({placeholders})
                ORDER BY t.name COLLATE NOCASE
                """,
                asset_ids,
            )
            results: dict[str, list[dict]] = {}
            for asset_id, name, color in cursor.fetchall():
                results.setdefault(asset_id, []).append({"name": name, "color": color})
            return results

    def get_tags_for_asset(self, asset_id: str) -> list[dict]:
        results = self.get_tags_for_assets([asset_id])
        return results.get(asset_id, [])
            
    @contextmanager
    def conn_context(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()
