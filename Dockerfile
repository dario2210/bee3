FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data /app/results

ENV PYTHONPATH=/app

EXPOSE 8061

CMD ["uvicorn", "bee3_dashboard:app", "--host", "0.0.0.0", "--port", "8061"]
