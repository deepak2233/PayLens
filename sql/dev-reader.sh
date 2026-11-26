#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set reader_password="$PAYLENS_PG_READER_PASSWORD" <<'SQL'
CREATE ROLE paylens_reader LOGIN PASSWORD :'reader_password';
GRANT USAGE ON SCHEMA public TO paylens_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO paylens_reader;
SQL
