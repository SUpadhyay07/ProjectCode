# Use the official OpenJDK 17 JDK slim image as a parent image
FROM openjdk:17-jdk-slim

# Set the JAVA_HOME environment variable
ENV JAVA_HOME /usr/local/openjdk-17

# Install Python 3 and necessary dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the Python script into the container
COPY Habyt_Task.py /app

# Install Python dependencies
RUN pip3 install --no-cache-dir pyspark requests pandas

# Run the Python script when the container launches
CMD ["python3", "Habyt_Task.py"]
