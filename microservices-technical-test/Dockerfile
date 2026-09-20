FROM python:3.13 AS base

WORKDIR /app

RUN pip install uv

FROM base AS builder

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

FROM base AS runtime

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY main.py ./
COPY settings.py ./
COPY application ./application
COPY __init__.py ./
COPY domain ./domain
COPY infrastructure ./infrastructure
COPY ioc ./ioc

# Gives ownership of the app directory to the microservices user
RUN chown -R microservices:microservices /app

# Create a microservices user with UID 1234 and GID 1234
RUN groupadd -g 1234 microservices && \
    useradd -m -u 1234 -g microservices microservices

# Switch to the microservices user
USER microservices

EXPOSE 8000

CMD [ "/app/.venv/bin/uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000" ]