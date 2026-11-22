.PHONY: demo api test prod lint clean

demo:            ## run the one-command demo (SQLite, no infra)
	python cli.py demo

api:             ## serve dashboard + API on :8000
	python cli.py api

test:            ## run the unit tests
	pytest -q

prod:            ## start the Kafka + Postgres stack (needs Docker)
	docker compose up --build

produce:         ## push the synthetic dataset through Kafka
	docker compose run producer

clean:
	rm -f paylens_demo.db paylens_demo_report.json
	find . -name __pycache__ -type d -exec rm -rf {} +
