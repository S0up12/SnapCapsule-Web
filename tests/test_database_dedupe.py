from __future__ import annotations

from datetime import datetime
from pathlib import Path

from core.database.schema import DatabaseManager
from core.models import MediaAsset, Message


def test_database_deduplicates_messages_users_and_links(tmp_path: Path) -> None:
    db = DatabaseManager(tmp_path / "app_state.db")
    asset_path = tmp_path / "memories" / "2026-03-06_ABC-main.mp4"
    asset_path.parent.mkdir(parents=True, exist_ok=True)
    asset_path.write_bytes(b"video")

    db.upsert_user("owner.snap", "Owner")
    db.add_asset(
        MediaAsset(
            asset_id="memories:2026-03-06_ABC",
            file_path=str(asset_path),
            file_type="video",
            file_size=asset_path.stat().st_size,
            created_at=datetime(2026, 3, 6, 22, 58, 48),
            overlay_path=None,
        )
    )

    message = Message(
        sender="friend.snap",
        content="",
        timestamp=datetime(2026, 3, 6, 22, 58, 48),
        msg_type="VIDEO",
        media_refs=[str(asset_path)],
    )

    db.add_message("friend.snap", message)
    db.add_message("friend.snap", message)

    with db.conn_context() as conn:
        message_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        link_count = conn.execute("SELECT COUNT(*) FROM message_media").fetchone()[0]
        user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    assert message_count == 1
    assert link_count == 1
    assert user_count == 2
