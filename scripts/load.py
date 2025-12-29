from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import json
import pandas as pd
from scripts.config import DB_URI, KAFKA_BOOTSTRAP, KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_TOPIC

def load():

    # db connection
    engine = create_engine(DB_URI)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=KAFKA_API_KEY,
        sasl_plain_password=KAFKA_API_SECRET,
        value_deserializer=lambda m: json.loads(m),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000
    )

    # process each message
    for message in consumer:
        data = message.value
        df = pd.DataFrame([{
            'symbol': data['symbol'],
            'open': data['open'],
            'high': data['high'],
            'low': data['low'],
            'close': data['close'],
            'volume': data['volume'],
            'dividends': data['dividends'],
            'stock_splits': data['stock_splits']
        }])

        # insert into db
        with engine.connect() as conn:
            for _, row in df.iterrows():
                conn.execute(
                    text("""
                        INSERT INTO stock_data (symbol, open, high, low, close, volume, dividends, stock_splits)
                        VALUES (:symbol, :open, :high, :low, :close, :volume, :dividends, :stock_splits)
                    """),
                    **row.to_dict()
                )
            conn.commit()

        print('data loaded into db')