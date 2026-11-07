# Superset with the Trino driver, connected to the lakehouse catalog on start.
FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir "trino[sqlalchemy]==0.340.0"
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh
USER superset
CMD ["/app/bootstrap.sh"]
