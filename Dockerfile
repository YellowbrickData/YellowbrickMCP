FROM python:3.12-slim

WORKDIR /app

# Copy the Python server script
COPY index.py .
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "index.py"]
