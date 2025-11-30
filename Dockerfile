FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# copy requirements first for caching
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

COPY app /app/app

# Expose Prometheus metrics port and FastAPI port
EXPOSE 9600 9110

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9110"]
