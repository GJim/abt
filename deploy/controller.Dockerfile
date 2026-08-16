FROM node:24-bookworm-slim AS web-build

WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM python:3.13-slim

WORKDIR /opt/abt
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/abt/.venv/bin:${PATH}"

COPY pyproject.toml uv.lock ./
COPY abt/ ./abt/
RUN pip install --no-cache-dir uv && uv sync --frozen --no-dev
COPY --from=web-build /src/web/dist /opt/abt/web

CMD ["/opt/abt/.venv/bin/uvicorn", "abt.controlplane.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
