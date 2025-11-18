FROM python:3.11-slim

# Force UTC in the container
ENV TZ=UTC
RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
 && ln -fs /usr/share/zoneinfo/$TZ /etc/localtime \
 && dpkg-reconfigure -f noninteractive tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App
COPY app.py .

# Run as non-root
RUN useradd -m runner
USER runner

# Defaults (overridable)
ENV POST_MINUTE=5
ENV POST_ON_START=true

# Simple healthcheck: DNS resolve Slack
HEALTHCHECK --interval=1m --timeout=10s --start-period=30s --retries=3 \
  CMD python -c "import socket,sys; socket.getaddrinfo('hooks.slack.com',443); sys.exit(0)"

CMD ["python", "app.py"]