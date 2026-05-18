# Multi-language BigQuery client image for the conformance suite.
#
# Each language client is installed in its own build stage; the final runtime
# stage carries only the language interpreters/JRE plus the resolved client
# libraries (no compilers, package managers or build tooling). All stages
# share one Debian base so the artifacts copied into the runtime stage are
# ABI-compatible with the interpreters installed there.

ARG BASE=debian:bookworm-slim

# ---------------------------------------------------------------------------
# Python client dependencies
# ---------------------------------------------------------------------------
FROM ${BASE} AS python-deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY clients/python/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages \
        --target /deps/python -r /tmp/requirements.txt

# ---------------------------------------------------------------------------
# Ruby client dependencies
# ---------------------------------------------------------------------------
FROM ${BASE} AS ruby-deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        ruby ruby-dev build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY clients/ruby/gems.txt /tmp/gems.txt
RUN gem install --no-document --install-dir /deps/ruby \
        $(awk -F: '{print $1, "-v", $2}' /tmp/gems.txt)

# ---------------------------------------------------------------------------
# PHP client dependencies
# ---------------------------------------------------------------------------
FROM ${BASE} AS php-deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        php-cli php-curl php-xml php-mbstring php-zip composer unzip git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY clients/php/composer.json /deps/php/composer.json
RUN composer install --no-dev --no-interaction --no-progress \
        --working-dir=/deps/php

# ---------------------------------------------------------------------------
# Node.js client dependencies
# ---------------------------------------------------------------------------
FROM ${BASE} AS node-deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        nodejs npm ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY clients/node/package.json /deps/node/package.json
RUN cd /deps/node && npm install --omit=dev --no-audit --no-fund

# ---------------------------------------------------------------------------
# Java client: build a self-contained runner jar
# ---------------------------------------------------------------------------
FROM ${BASE} AS java-build
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless maven ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY clients/java/pom.xml ./pom.xml
RUN mvn -q -DskipTests dependency:go-offline || true
COPY clients/java/src ./src
RUN mvn -q -DskipTests package \
    && mkdir -p /deps/java \
    && cp target/runner.jar /deps/java/runner.jar

# ---------------------------------------------------------------------------
# Runtime: interpreters + resolved client libraries only
# ---------------------------------------------------------------------------
FROM ${BASE}
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 ruby php-cli php-curl php-xml php-mbstring nodejs \
        openjdk-17-jre-headless ca-certificates tzdata curl gnupg \
    && rm -rf /var/lib/apt/lists/*

# The `bq` CLI ships with the Google Cloud SDK.
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-deps /deps/python /deps/python
COPY --from=ruby-deps   /deps/ruby   /deps/ruby
COPY --from=php-deps    /deps/php    /deps/php
COPY --from=node-deps   /deps/node   /deps/node
COPY --from=java-build  /deps/java   /deps/java

COPY clients/python/runner.py /app/python/runner.py
COPY clients/ruby/runner.rb   /app/ruby/runner.rb
COPY clients/php/runner.php   /app/php/runner.php
COPY clients/node/runner.js   /app/node/runner.js
COPY clients/bq/runner.py     /app/bq/runner.py
COPY cases/cases.json         /app/cases.json
COPY entrypoint.sh            /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV PYTHONPATH=/deps/python \
    GEM_HOME=/deps/ruby \
    GEM_PATH=/deps/ruby \
    NODE_PATH=/deps/node/node_modules \
    PROJECT_ID=test \
    CASES_FILE=/app/cases.json \
    REPORT_FILE=/work/report.json \
    CLOUDSDK_AUTH_ACCESS_TOKEN=emulator \
    CLOUDSDK_CORE_DISABLE_PROMPTS=1

WORKDIR /work
ENTRYPOINT ["/app/entrypoint.sh"]
