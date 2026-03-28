.PHONY: start stop restart logs topics ps clean

start:
	docker compose up -d

stop:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f

ps:
	docker compose ps

topics:
	docker exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic raw.ticks \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists
	
	docker exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic raw.news \
		--partitions 1 \
		--replication-factor 1 \
		--if-not-exists
	
	docker exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic raw.dlq \
		--partitions 1 \
		--replication-factor 1 \
		--if-not-exists
	
	docker exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic processed.ohlcv \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists

list-topics:
	docker exec kafka kafka-topics --list \
		--bootstrap-server localhost:9092


clean:
	docker compose down -v