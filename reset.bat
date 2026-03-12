@echo off
setlocal

docker compose down

if not exist data\imports\pending mkdir data\imports\pending
move data\imports\processed\* data\imports\pending\ >nul 2>&1
move data\imports\failed\* data\imports\pending\ >nul 2>&1

if exist data\database\app_state.db del /f /q data\database\app_state.db
if exist data\imports\extracted rmdir /s /q data\imports\extracted

docker compose up -d --build
