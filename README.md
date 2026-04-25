<h1>Stock Market Streaming Data Pipeline</h1>
This project implements a streaming data pipeline to get real-time data on the prices of stock market symbols, such as AAPL, MSFT, NVDA, etc.

<h2>Project Structure</h2>

```text
.
├── api/                # FastAPI for real-time data monitoring
├── config/             # Airflow configuration
├── dags/               # Airflow DAGs definition
├── observability/      # Prometheus yml file definitions for observability
├── plugins/            # Additional features
├── processors/         # Kafka processors (consumers)
├── producers/          # Kafka producers (including base_producer class)
├── quality/            # Data quality monitoring
├── schemas/            # Avro schemas defintion
├── tests/              # Tests definitions
├── .env.example        # Example of .env file (make sure to configure yourself)
├── docker-compose.yml  # docker containers definitions
├── Makefile            # useful shortcuts not to write too many command lines
├── README.md           # This file
└── requirements.txt    # package requirements
```


<h2>Streaming Pipeline</h2>

<h2>docker-compose</h2>

<h2>Airflow DAGs</h2>

<h2>Observability</h2>

<h2>
