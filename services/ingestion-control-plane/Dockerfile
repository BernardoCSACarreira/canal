# syntax=docker/dockerfile:1
# Default local-stack image; Python baseline matches .python-version / CI.
FROM python:3.12-bookworm
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir pip setuptools wheel \
    && pip install --no-cache-dir -e .
EXPOSE 8091
CMD ["uvicorn", "canal_control_plane.app:app", "--host", "0.0.0.0", "--port", "8091"]
