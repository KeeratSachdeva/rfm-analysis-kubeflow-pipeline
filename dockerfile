# Use a lightweight Python base image.
FROM python:3.9-slim

# Set working directory.
WORKDIR /app

# Install dependencies.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code.
COPY rfc_predictions_flask.py .

# Expose the port Flask runs on.
EXPOSE 8080

# Set the entrypoint.
CMD ["python", "rfc_predictions_flask.py"]