FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

WORKDIR /app

# --------------------------------------------------------
# Install system dependencies
# --------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        software-properties-common \
        curl \
        wget \
        ffmpeg \
        python3 \
        python3-pip \
        python3-venv \
        python3-dev \
        build-essential \
        ca-certificates \
        libdvd-pkg \
	libdvdread8 \
        libdvdnav4 \
        libdvdcss2 \
    && dpkg-reconfigure libdvd-pkg \
    && rm -rf /var/lib/apt/lists/*

# --------------------------------------------------------
# Install HandBrakeCLI (Ubuntu has it in the repo)
# --------------------------------------------------------
RUN apt-get update && apt-get install -y handbrake-cli && \
    rm -rf /var/lib/apt/lists/*

# --------------------------------------------------------
# Install Poetry
# --------------------------------------------------------
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# --------------------------------------------------------
# Copy dependency files
# --------------------------------------------------------
COPY pyproject.toml poetry.lock* /app/

# --------------------------------------------------------
# Install Python dependencies
# --------------------------------------------------------
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes || \
    echo "ffmpeg-python" > requirements.txt && \
    pip3 install -r requirements.txt

# --------------------------------------------------------
# Copy your application
# --------------------------------------------------------
COPY . /app/

# Make run shell script executable
RUN chmod +x /app/scripts/run_encoder.sh

# --------------------------------------------------------
# Default command
# --------------------------------------------------------
CMD ["./app/scripts/run_encoder.sh"]

