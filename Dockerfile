FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir -e .

# Add config.yaml (override at runtime via volume mount)
COPY config.yaml ./

EXPOSE 3001

CMD ["python", "-m", "agent_channel_bridge"]
