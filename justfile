all:
  docker compose -f docker-compose.postgres.yml -f docker-compose.chat.yml -f docker-compose.ui.yml -f docker-compose.neo4j.yml -f docker-compose.falkordb.yml down
  docker compose -f docker-compose.postgres.yml -f docker-compose.chat.yml -f docker-compose.ui.yml -f docker-compose.neo4j.yml -f docker-compose.falkordb.yml up --build --pull always