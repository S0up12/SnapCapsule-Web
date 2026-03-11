# SnapCapsule Web

Self-hosted web rewrite of the original SnapCapsule desktop app.

The project ingests Snapchat export ZIPs, stores parsed metadata in SQLite, exposes a FastAPI API for chats and memories, and serves a React frontend designed for home-lab deployment.

## Stack

- Backend: FastAPI, Python 3.10
- Frontend: React, Vite, TailwindCSS
- Media processing: Pillow, OpenCV, FFmpeg
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

Drop Snapchat export ZIP files into `./data/imports`, then trigger ingestion from the dashboard or with:

```bash
curl -X POST http://localhost:8000/api/ingest/
```

## Useful Endpoints

- `GET /` - backend health check
- `GET /api/ingest/status` - archive status and counts
- `POST /api/ingest/` - ingest the newest valid Snapchat ZIP in the imports directory
- `GET /api/memories/` - paginated memories API
- `GET /api/chats/` - conversations list
- `GET /api/chats/{account_id}/messages` - paginated messages for one conversation
- `GET /media/cache/...` - generated thumbnails
- `GET /media/raw/...` - extracted raw media files

## Frontend Development

Run the backend with Docker, then start the frontend dev server:

```bash
cd frontend
npm install
npm run dev
```

Vite proxies `/api` and `/media` to the backend at `http://localhost:8000`.

## Project Layout

```text
src/
  core/       Backend ingestion, database, media processing
  routers/    FastAPI route modules
  main.py     FastAPI app entrypoint
frontend/
  src/        React application
  Dockerfile  Production frontend image
Dockerfile    Backend image
docker-compose.yml
```

## Status

Current state:

- Desktop-specific PyQt code has been removed from the runtime path.
- Backend API routes for ingestion, memories, chats, and static media are in place.
- Frontend routing, dashboard shell, and Dockerized Nginx build are in place.

The next major step is building the full memories and chats interfaces on top of the existing APIs.
