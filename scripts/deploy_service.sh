#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/gabo/portfolio/projects/10-vehiculos"
SERVICE_NAME="portfolio-vehiculos"
SERVICE_SRC="${PROJECT_DIR}/deploy/systemd/${SERVICE_NAME}.service"
SERVICE_DST="/etc/systemd/system/${SERVICE_NAME}.service"

NGINX_SRC="${PROJECT_DIR}/deploy/nginx/vehiculos.omar-xyz.shop.conf"
NGINX_AVAIL="/etc/nginx/sites-available/vehiculos.omar-xyz.shop.conf"
NGINX_ENABLED="/etc/nginx/sites-enabled/vehiculos.omar-xyz.shop.conf"

if [[ ! -f "${SERVICE_SRC}" ]]; then
  echo "Missing service file: ${SERVICE_SRC}"
  exit 1
fi

if [[ ! -f "${NGINX_SRC}" ]]; then
  echo "Missing nginx file: ${NGINX_SRC}"
  exit 1
fi

if [[ ! -f "${PROJECT_DIR}/.env" ]]; then
  echo "Warning: .env not found at ${PROJECT_DIR}/.env (service will use defaults)."
fi

if [[ $EUID -ne 0 ]]; then
  echo "Run this script with sudo."
  exit 1
fi

cp "${SERVICE_SRC}" "${SERVICE_DST}"
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"

cp "${NGINX_SRC}" "${NGINX_AVAIL}"
if [[ ! -L "${NGINX_ENABLED}" ]]; then
  ln -s "${NGINX_AVAIL}" "${NGINX_ENABLED}"
fi

nginx -t
systemctl reload nginx

echo "Done. Service and nginx config applied."
