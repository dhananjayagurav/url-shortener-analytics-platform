FROM spark:3.5.9-python3

USER root

RUN pip install --no-cache-dir "pydantic-settings==2.15.0" "pydantic==2.13.5"

RUN usermod -d /opt/spark/work-dir spark

USER spark

WORKDIR /opt/spark/work-dir
COPY phase2-spark/src/ ./phase2-spark/src/
ENV PYTHONPATH=/opt/spark/work-dir/phase2-spark/src

ENV HOME=/opt/spark/work-dir