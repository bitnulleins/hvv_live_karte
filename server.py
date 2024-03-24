import os
import time

from flask import Flask, render_template, Response, jsonify
from kafka import KafkaConsumer
from src.etl_live import ETLPipeline
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from src.etl.propagate import propagate

load_dotenv()

app = Flask(__name__)

@app.route('/')
def index():
    """
    Frontpage rendering (interactive OSM)
    """
    # ETL Thread wenn ohne Luigi
    #threading.Thread(target=ETLPipeline()).start()

    return(render_template('index.html'))

@app.route('/topic/<topic>')
def consume_message(topic):
    """
    Consumer endpoint for Kafka communication
    """
    consumer = KafkaConsumer(topic, bootstrap_servers=os.environ['KAFKA_HOST'])

    def eventStream():
        for message in consumer:
            # Specific response format for event-stream is required (!)
            yield 'data: {}\n\n'.format(message.value.decode())

    return Response(eventStream(), mimetype='text/event-stream')

@app.route('/analyze')
def analyze():
    """
    Analyze mongo db with current state and save value to heatmap.json
    """
    try:
        url = f"mongodb://{os.environ['MONGO_USER']}:{os.environ['MONGO_PASSWD']}@{os.environ['MONGO_HOST']}:{os.environ['MONGO_PORT']}/DAD.{os.environ['KAFKA_TOPIC']}?authSource=admin"
        
        # Connect to Spark
        spark = SparkSession \
            .builder \
            .appName("fahrtendb") \
            .master('local')\
            .config("spring.data.mongodb.authentication-database=admin") \
            .config("spark.mongodb.input.uri", url) \
            .config("spark.mongodb.output.uri", url) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
    except:
        raise Exception("[WARNING] Spark not connected.")

    df = spark.read\
        .format('com.mongodb.spark.sql.DefaultSource')\
        .load()

    print(f"[INFO] Unique entries in Mongo: {df.count()}")

    # Aggregate and save as json
    data = df.filter((df['timestamp'] > int(time.time()-86400)) & (df['realtimeDelay'] > 0)).select(
        F.expr("tracks.end[0]").alias("lat"), # Endtsationsverspätung
        F.expr("tracks.end[1]").alias("lon"),
        F.expr("realtimeDelay").alias("delay")
        # F.max vs. F.avg austauschbar
    ).withColumn("avg_delay", F.max("delay").over( Window.partitionBy("lon","lat") )).drop("delay").dropDuplicates(['lat','lon']).toPandas()

    # MinMax Scaler
    #data['avg_delay'] = data['avg_delay'] / max(data['avg_delay'])

    # Save to file for frontend
    data.to_json('./static/heatmap.json', orient='records')

    # Get color map for heatmap by quantiles: https://github.com/mourner/simpleheat/blob/gh-pages/simpleheat.js
    color_map = {
        'color-1': f"{data['avg_delay'].quantile(q=0.2):.2f} Minuten",
        'color-2': f"{data['avg_delay'].quantile(q=0.4):.2f} Minuten",
        'color-3': f"{data['avg_delay'].quantile(q=0.6):.2f} Minuten",
        'color-4': f"{data['avg_delay'].quantile(q=0.8):.2f} Minuten",
        'color-5': f"{data['avg_delay'].quantile(q=0.9):.2f} Minuten",
    }

    return jsonify(color_map)

if __name__ == '__main__':
    # Flask App  (Frontend)
    app.run(debug=True, port=5001)