FROM python:3.11-slim

# Install Chromium + chromedriver for ARM/AMD (works for your t4g.medium EC2)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        chromium \
        chromium-driver \
        fonts-liberation \
        ca-certificates \
        curl \
        unzip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App files
COPY global_monitor.py ./global_monitor.py
RUN mkdir -p src/global
COPY global_config.yaml ./src/global/global_config.yaml

# Default env (you can override in docker run)
ENV AWS_REGION=ap-northeast-2 \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    USE_SECRETS_MANAGER=true \
    SECRET_NAME=arn:aws:secretsmanager:ap-northeast-2:702147360356:secret:qobs/healthcheck-fwN15E

# Run the script
CMD ["python", "global_monitor.py"]
