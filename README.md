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

The streaming pipeline is designed to retrieve stock market and news information from different APIs.

<h3>Producers</h3>

- Real-Time Alpaca Producer: This producer is in charge of asking stock prices for the symbols previously defined at a real-time processing. It is important to highligh that this producer only works actively when stock markets are opened, which for the American standard is meant to be from 9:30 a.m. to 4:00 p.m. ET. on weekdays. Otherwise, the producer will not obtain any information to publish in the topics.

- Yahoo Rest Producer: This producer is meant to be an alternative to the Alpaca Producer in case of failures. It is meant to retrieve information on symbols prices every 5 minutes and it works actively even when stock markets are not active, though the prices will remain the same.

- News Producer: This producer obtains information on all the latest news that involve the symbols previously defined. News with a title like "Apple presents its revenues for the last quarter with a dramatic increase", and that contain tags that link the symbols that were defined (AAPL for the case of Apple) will be published into a specific topic by this producer.

- Base Producer: This is not a specific producer, but a base class from which all the previous producers will be inheriting properties and methods. It is build appart for better readability.


<h3>Processors</h3>

- Sentiment Agent: This processor (or consumer) is in charge of analyzing the registers published into the news topic from the News Producer. It is meant to analyze the title and description of the news published using the Vader Sentiment library, and lately define whether the new is positive, negative or neutral. This information is meant to work as a parameter to define models and estimations for investment strategies.

- OHLCV Agent: OHLCV stands for Open, High, Low, Close, Volumen, and these are parameters that are constant when analyzing stock prices in certain ranges. This consumer will get the information on prices that is published in the raw_ticks topic used by the Alpaca and Yahoo producers. During a 1 minute window, this consumer takes the different prices that it receives and comprises into a OHLCV register that is lately saved into a Timescale Database (meant for real-time information which is drop after some days).

- DLQ Agent: As its name indicates, this consumer will take care of the wrongly published registers and publish them into a raw.dlq topic. It also save the information into the Timescale Database to analyze them lately.

- Anomaly Agent: This processor is subscribed to the raw.ticks topic (symbols prices) and will analyze whether the prices have a natural behaviour or are anomalous. If they're anomalous, then the information is saved into the Timescale Database.


<h3>Topics</h3>

<h2>docker-compose</h2>

<h2>Airflow DAGs</h2>

<h2>Observability</h2>

<h2>
