FROM python:3.12-slim

ARG STPSTONE_VERSION=latest

WORKDIR /app

COPY run_dist.py ./

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    jq && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir toml psycopg[binary]

RUN if [ "$STPSTONE_VERSION" = "latest" ]; then \
    pip install --no-cache-dir stpstone; \
    else \
    pip install --no-cache-dir stpstone==${STPSTONE_VERSION}; \
    fi

CMD python run_dist.py