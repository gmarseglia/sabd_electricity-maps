FROM apache/spark:3.5.5

# Set working directory
WORKDIR /opt/spark/work-dir

# Copy requirements first for caching
COPY requirements.txt .

# Install dependencies without cache
USER root
RUN python3 -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

USER spark

# docker build --no-cache --progress=plain -f spark/Dockerfile -t gm/spark spark/