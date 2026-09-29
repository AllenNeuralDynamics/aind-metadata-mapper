FROM python:3.10-bookworm
WORKDIR /app
ADD src ./src
ADD pyproject.toml .
ADD setup.py .

# Add git in case we need to install from branches
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
RUN pip install . --no-cache-dir
