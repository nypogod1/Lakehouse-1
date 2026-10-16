FROM apache/superset:latest
USER root
RUN . /app/.venv/bin/activate && \
    uv pip uninstall -y sqlalchemy-trino && \
    uv pip install --no-cache-dir "trino[sqlalchemy]"
USER superset