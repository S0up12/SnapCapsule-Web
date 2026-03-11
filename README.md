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

## Self-Hosting (Portainer / ZimaOS)

SnapCapsule Web is designed for home-lab deployment first. The easiest install path is Docker Compose, and the intended appliance-style workflow is Portainer or a Portainer-based environment such as ZimaOS.

### Portainer Stack Deployment

1. In Portainer, open `Stacks`.
2. Create a new stack from your Git repository or paste the repository Compose file.
3. Deploy the stack using the included `docker-compose.yml`.
4. After the containers start, open the frontend on port `3000`.

The backend stores everything under `/data` inside the container. You control where that data lives on the host through environment-variable-backed bind mounts.

### Environment Variables

The backend service supports three storage path variables:

- `DATABASE_DIR`
  Host path for SQLite and staged metadata
- `CACHE_DIR`
  Host path for thumbnails and browser-safe transcoded video cache
- `IMPORTS_DIR`
  Host path for incoming Snapchat archives and extracted media

In Portainer, add these as stack environment variables and point them at your preferred NAS or storage locations. Example host paths:

- `DATABASE_DIR=/mnt/storage/appdata/snapcapsule/database`
- `CACHE_DIR=/mnt/storage/appdata/snapcapsule/cache`
- `IMPORTS_DIR=/mnt/storage/media/snapchat-imports`

If you do not define them, Compose falls back to local project folders under `./data`.

## Setup

### Option 1: Docker Compose on a local machine

```bash
docker compose up -d --build
```

By default, persistent data is stored in:

- `./data/database`
- `./data/cache`
- `./data/imports`

To override those locations, copy `.env.example` to `.env` and set your host paths before starting the stack.

### Option 2: Portainer or ZimaOS

- Create a stack from this repository
- Set `DATABASE_DIR`, `CACHE_DIR`, and `IMPORTS_DIR` in the Portainer environment-variable UI if you want custom storage locations
- Deploy the stack

## Run

Once the stack is up:

1. Open the frontend at `http://<your-server>:3000`
2. Place Snapchat export files in your mapped imports directory
3. Start the import from the Dashboard
4. Browse memories and chats after ingestion completes

## Import Workflow

1. Drop a Snapchat export archive into your mapped imports directory.
   Supported formats: `.zip`, `.rar`, `.7z`
2. If you already extracted the export, place the extracted folder under the imports root, `extracted`, or `raw` inside that mapped location.
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

The project is release-oriented and container-first, but local development is still straightforward.

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
