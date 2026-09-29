# ===========================================================================
#  Image for the FastAPI serving layer.
#
#  Only the API's own dependencies are installed, not the whole of
#  requirements.txt: the producers' Kafka client, pyarrow and pytest have no
#  business in a container whose only job is to answer HTTP requests. Versions
#  are pinned to the same values requirements.txt uses, so the API behaves the
#  same whether it is run in Docker or with uvicorn on the host.
# ===========================================================================
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
      fastapi==0.115.4 \
      "uvicorn[standard]==0.32.0" \
      "psycopg[binary]==3.2.3" \
      prometheus-client==0.21.0 \
      structlog==24.4.0 \
      PyYAML==6.0.2

# Runs unprivileged. The API needs no write access to anything it mounts.
RUN useradd --create-home --uid 1000 api
USER api

# The code itself is bind-mounted by docker-compose rather than copied in, so an
# edit needs only a container restart. The trade-off -- the image is not a
# self-contained artefact of this code -- is the right one for a project that is
# developed and demonstrated on the same machine.
EXPOSE 8000
CMD ["uvicorn", "serving.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
