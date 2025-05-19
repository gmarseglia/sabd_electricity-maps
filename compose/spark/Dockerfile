FROM apache/spark:3.5.5

USER root

WORKDIR /opt/spark/conf

RUN echo "spark.eventLog.enabled true" >> spark-defaults.conf
RUN echo "spark.eventLog.dir file:///tmp/spark-events" >> spark-defaults.conf

# Set working directory
WORKDIR /opt/spark/work-dir

# Copy requirements first for caching
COPY requirements.txt .

# Install dependencies without cache
RUN python3 -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

USER spark

RUN mkdir /tmp/spark-events
# RUN chown -R spark:spark /tmp/spark-events

# docker build --no-cache --progress=plain -f spark/Dockerfile -t gm/spark spark/