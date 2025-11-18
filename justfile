all:
  docker compose -f docker-compose.postgres.yml -f docker-compose.chat.yml -f docker-compose.ui.yml -f docker-compose.neo4j.yml -f docker-compose.falkordb.yml -f docker-compose.educational-dbt-docs.yml down
  docker compose -f docker-compose.postgres.yml -f docker-compose.chat.yml -f docker-compose.ui.yml -f docker-compose.neo4j.yml -f docker-compose.falkordb.yml -f docker-compose.educational-dbt-docs.yml up --build --pull always

open_all:
  open -a "Google Chrome" "http://localhost:8502" "http://localhost:3000" "http://localhost:8501"

load_dbt_to_falkordb:
  dbt-graph-loader falkordb --manifest DbtEducationalDataProject/target/manifest.json --catalog DbtEducationalDataProject/target/catalog.json