#!/bin/sh
# Local development only: an admin/admin account and the lakehouse registered as a database.
set -e
superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin || true
superset init
superset set-database-uri --database_name lakehouse --uri "trino://lakehouse@trino:8080/lakehouse"
exec gunicorn --bind 0.0.0.0:8088 --workers 2 --timeout 120 "superset.app:create_app()"
