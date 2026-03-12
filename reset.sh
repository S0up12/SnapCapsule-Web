#!/usr/bin/env sh
set -eu

docker compose down

mkdir -p data/imports/pending

if [ -d data/imports/processed ]; then
  find data/imports/processed -maxdepth 1 -type f -exec mv {} data/imports/pending/ \; || true
fi

if [ -d data/imports/failed ]; then
  find data/imports/failed -maxdepth 1 -type f -exec mv {} data/imports/pending/ \; || true
fi

rm -f data/database/app_state.db
rm -rf data/imports/extracted

docker compose up -d --build
