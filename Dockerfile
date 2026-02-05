FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY bot.py .

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import http.client; conn = http.client.HTTPConnection('localhost', 8000); conn.request('GET', '/'); r = conn.getresponse(); exit(0 if r.status == 200 else 1)"

# Run
CMD ["python", "-u", "bot.py"]
