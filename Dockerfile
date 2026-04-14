FROM python:3.14-slim

WORKDIR /app

COPY pyproject.toml README.md LICENSE.md ./
RUN mkdir -p conductarr && touch conductarr/__init__.py conductarr/py.typed \
    && pip install --no-cache-dir . \
    && rm -rf conductarr

COPY conductarr/ ./conductarr/
RUN pip install --no-cache-dir --no-deps .

RUN groupadd -g 1000 conductarr \
    && useradd -u 1000 -g conductarr -m --no-log-init conductarr \
    && mkdir -p /config \
    && chown conductarr:conductarr /app /config

ENV PUID=1000
ENV PGID=1000
ENV UMASK=022
ENV TZ=UTC
ENV CONFIG_DIR=/config
ENV LOG_DIR=/config/logs

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

VOLUME /config

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["conductarr", "--help"]
