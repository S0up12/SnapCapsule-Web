from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

class MediaAsset(BaseModel):
    """Represents a physical file on disk."""
    asset_id: str  # Unique hash or Snap-provided ID
    file_path: str
    file_type: str  # 'image', 'video', 'audio', 'overlay'
    file_size: int
    created_at: Optional[datetime] = None
    overlay_path: Optional[str] = None  # <--- NEW FIELD

class Message(BaseModel):
    """Validated chat message structure."""
    id: Optional[int] = None
    sender: str
    content: str
    timestamp: datetime
    msg_type: str = "TEXT"
    media_refs: List[str] = Field(default_factory=list) # List of asset_ids
    source: str = "chat"

class Conversation(BaseModel):
    """A collection of messages with a specific friend."""
    username: str
    display_name: Optional[str] = None
    messages: List[Message]
