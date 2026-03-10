FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy uv installer
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Copy project files
COPY . /app/

# Install dependencies with uv
RUN uv sync --frozen

# Set entrypoint
ENTRYPOINT ["uv", "run", "--package", "capture__jae_backup_billingpay", "prefect", "flow-run", "execute"]
