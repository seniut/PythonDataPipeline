FROM python:3.10

# Create a non-root user and set the working directory
RUN useradd --create-home --shell /bin/false appuser
WORKDIR /home/appuser

# Copy files and install dependencies as root
COPY . .
RUN pip install --no-cache-dir -r ./requirements.txt

# Change to non-root user
USER appuser

# Set the default command for the container
CMD ["python", "./etl_flow.py"]

