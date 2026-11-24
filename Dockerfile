FROM python:3.11.14-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=Asia/Shanghai NETEASE_DATA_DIR=/data NETEASE_CONFIG_FILE=/config/config.json NETEASE_API_BASE=http://netease-api:3000 NETEASE_AUTO_START_API=0
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd -u 10001 -m scanner
COPY engine.py server.py m3_mode.py m3_artist_names.json slow_retry.py record_export.py probe.py verify_migration.py ./
COPY templates ./templates
COPY static ./static
USER scanner
EXPOSE 5000
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/health',timeout=3)"
CMD ["python", "server.py"]
