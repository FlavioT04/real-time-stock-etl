import yfinance as yf
from kafka import KafkaProducer
import json
from scripts.config import KAFKA_BOOTSTRAP, KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_TOPIC

def ingest():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=KAFKA_API_KEY,
        sasl_plain_password=KAFKA_API_SECRET,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # stocks to track
    stocks = ['AAPL', 'MSFT', 'GOOG', 'AMZN']

    # send data to topic
    for stock in stocks:
        data = yf.Ticker(stock).history(period='1d', interval='1m').tail(1).to_dict('records')[0]
        data['symbol'] = stock
        producer.send(KAFKA_TOPIC, value=data)
        print(f'{stock} data sent to kafka')

    producer.flush()
    producer.close()