FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt && \
    pip uninstall -y opencv-python opencv-contrib-python || true && \
    pip install --no-cache-dir opencv-python-headless

COPY . ./backend

CMD uvicorn backend.app:app --host 0.0.0.0 --port ${PORT:-8000}
