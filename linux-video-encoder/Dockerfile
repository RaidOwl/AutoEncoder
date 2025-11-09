FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

WORKDIR /app

# System deps (ffmpeg for ffmpeg-python)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg curl && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Copy only dependency files for caching
COPY pyproject.toml poetry.lock* /app/

# Export dependencies and install with pip
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes || \
    echo "ffmpeg-python" > requirements.txt && \
    pip install -r requirements.txt

# Copy project files
COPY . /app/

# Default run command
CMD ["python", "main.py"]
