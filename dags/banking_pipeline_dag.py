from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from kafka import KafkaProducer

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def ingest_csv_to_kafka(**context):
    """Read CSV from /opt/airflow/data/raw/ and publish each row to Kafka."""
    import glob, os

    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
    )

    files = glob.glob('/opt/airflow/data/raw/*.csv')
    if not files:
        raise FileNotFoundError("No CSV files found in /opt/airflow/data/raw/")

    total = 0
    for filepath in files:
        df = pd.read_csv(filepath)
        df.columns = [c.strip().lower() for c in df.columns]

        for _, row in df.iterrows():
            record = row.where(pd.notnull(row), None).to_dict()
            producer.send(
                topic='transactions',
                key=str(record.get('transaction_id', 'unknown')),
                value=record
            )
            total += 1

        # Move processed file
        done_dir = '/opt/airflow/data/processed'
        os.makedirs(done_dir, exist_ok=True)
        os.rename(filepath, os.path.join(done_dir, os.path.basename(filepath)))

    producer.flush()
    producer.close()
    print(f"Published {total} records to Kafka topic 'transactions'")
    return total


def load_raw_to_postgres(**context):
    """Consume raw topic and dump into raw_transactions table."""
    import psycopg2
    from kafka import KafkaConsumer

    conn = psycopg2.connect(
        host='postgres', dbname='banking_dw',
        user='airflow', password='airflow'
    )
    cur = conn.cursor()

    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='raw-loader',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )

    insert_sql = """
        INSERT INTO raw_transactions (
            transaction_id, client_id, date_transaction, montant, devise,
            taux_change_eur, montant_eur, categorie, produit, agence,
            type_operation, statut, score_credit_client, segment_client,
            solde_avant, taux_interet
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """

    count = 0
    for msg in consumer:
        r = msg.value
        cur.execute(insert_sql, (
            r.get('transaction_id'), r.get('client_id'), r.get('date_transaction'),
            r.get('montant'), r.get('devise'), r.get('taux_change_eur'),
            r.get('montant_eur'), r.get('categorie'), r.get('produit'),
            r.get('agence'), r.get('type_operation'), r.get('statut'),
            r.get('score_credit_client'), r.get('segment_client'),
            r.get('solde_avant'), r.get('taux_interet')
        ))
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    consumer.close()
    print(f"Loaded {count} rows into raw_transactions")


with DAG(
    dag_id='banking_transaction_pipeline',
    default_args=default_args,
    description='CSV → Kafka → Spark → PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['banking', 'kafka', 'spark'],
) as dag:

    t1_ingest = PythonOperator(
        task_id='ingest_csv_to_kafka',
        python_callable=ingest_csv_to_kafka,
    )

    t2_raw_load = PythonOperator(
        task_id='load_raw_to_postgres',
        python_callable=load_raw_to_postgres,
    )

    t3_spark_transform = BashOperator(
            task_id='spark_transform',
            bash_command="""
                docker exec banking_transaction_pipeline-spark-master-1 \
                /opt/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
                /opt/spark-apps/transform.py
            """,
        )

    t1_ingest >> t2_raw_load >> t3_spark_transform