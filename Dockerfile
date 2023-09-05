FROM quay.io/astronomer/astro-runtime:9.0.0

RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir soda-core-postgres \
    pip install --no-cache-dir soda-core-scientific \
    && deactivate