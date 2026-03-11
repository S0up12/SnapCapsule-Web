# SnapCapsule Web

SnapCapsule Web is a self-hosted Snapchat archive browser built around a FastAPI backend and a React frontend. It ingests Snapchat exports, stores parsed metadata in SQLite, generates thumbnails and browser-safe video derivatives, and exposes a UI for dashboard controls, memories browsing, and chat reading.

## Features

- Ingest Snapchat exports from `.zip`, `.rar`, and `.7z`
- Auto-detect pre-extracted exports in the imports directory
- Cancel long-running imports from the dashboard
- Merge/stage export JSON files and parse memories, chats, and snap history
- Serve generated thumbnails and raw media through FastAPI static mounts
- Transcode incompatible videos to browser-safe H.264 cache files on demand
- Browse memories in a responsive gallery with a shared full-screen lightbox
- Browse conversations in a two-pane chat reader with inline media expansion

## Stack

- Backend: FastAPI, Python 3.10
- Frontend: React 19, Vite, Tailwind CSS
- Database: SQLite
- Media tooling: Pillow, OpenCV, FFmpeg, py7zr, rarfile
- Deployment: Docker Compose

## Services

- Backend: `http://localhost:8000`
- Frontend: `http://localhost:3000`

## Quick Start

```bash
docker compose up -d --build
```

Persistent data is stored in:

- `./data/database`
- `./data/cache`
- `./data/imports`

## Import Workflow

1. Drop a Snapchat export archive into `./data/imports`.
   Supported formats: `.zip`, `.rar`, `.7z`
2. If you already extracted the export, place the extracted folder under `./data/imports`, `./data/imports/extracted`, or `./data/imports/raw`.
3. Start the import from the dashboard or via:

```bash
curl -X POST http://localhost:8000/api/ingest/
```

4. Cancel a running import if needed:

```bash
curl -X POST http://localhost:8000/api/ingest/cancel
```

## API Overview

- `GET /`  
  Health check plus resolved storage paths
- `GET /api/ingest/status`  
  Import state and archive counts
- `POST /api/ingest/`  
  Import the latest valid archive or detected extracted export
- `POST /api/ingest/cancel`  
  Request cancellation of the active import
- `GET /api/memories/?skip=0&limit=50`  
  Paginated memories API
- `GET /api/chats/`  
  Conversation list
- `GET /api/chats/{account_id}/messages?skip=0&limit=50`  
  Paginated messages for one conversation
- `GET /media/cache/...`  
  Generated thumbnails and browser-safe video cache files
- `GET /media/raw/...`  
  Extracted raw media files

## Development

### Backend

Run the backend directly:

```bash
pip install -r requirements.txt
cd src
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Frontend

With the backend running on port `8000`:

```bash
cd frontend
npm install
npm run dev
```

Vite proxies `/api` and `/media` to the backend at `http://localhost:8000`.

## Project Layout

```text
src/
  core/
    database/      SQLite access layer
    services/      Ingestion and media processing
    utils/         Path and media helper utilities
  routers/         FastAPI route modules
  main.py          FastAPI app entrypoint
frontend/
  src/
    components/    Shared React UI components
    views/         Dashboard, memories, and chats views
  Dockerfile       Production frontend image
Dockerfile         Backend image
docker-compose.yml
```

## Notes

- FFmpeg is required for thumbnail extraction fallback and browser-safe video transcoding.
- Generated cache files live under `./data/cache`.
- The frontend and backend are designed to run on the same origin in Docker, with Vite proxy support for local frontend development.
