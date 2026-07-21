FROM python:3.12-slim

# Prevent Python from buffering logs
ENV PYTHONUNBUFFERED=True

# Set working directory
WORKDIR /app

# Copy requirements first
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application
COPY . .

# Cloud Run uses port 8080
ENV PORT=8080

# Start FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]