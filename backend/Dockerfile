# Use a lightweight official Python image
FROM python:3.11-slim

# Install essential system dependencies for compiling wheels and database drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working execution directory inside the container
WORKDIR /app

# Copy over package configurations and upgrade the manager
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Run Uvicorn pointing directly to your app modules
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]