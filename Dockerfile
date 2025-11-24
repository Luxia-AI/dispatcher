# dispatcher/Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# copy requirements first for caching
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

COPY dispatcher /app/dispatcher

# Expose Prometheus metrics port and FastAPI port
EXPOSE 9600 9110

CMD ["uvicorn", "dispatcher.app.main:app", "--host", "0.0.0.0", "--port", "9110", "--reload"]
