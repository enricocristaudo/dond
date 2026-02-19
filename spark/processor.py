import os
import json
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import from_json, col, udf, struct, to_json # type: ignore
from pyspark.sql.types import StringType, StructType, FloatType # type: ignore
from transformers import pipeline # type: ignore
import torch # type: ignore

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "raw-articles")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "classified-articles")

spark = SparkSession.builder \
    .appName("DuceDetectorML") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("id", StringType()) \
    .add("query", StringType()) \
    .add("title", StringType()) \
    .add("description", StringType()) \
    .add("url", StringType()) \
    .add("source", StringType()) \
    .add("publishedAt", StringType()) \
    .add("full_text", StringType())

classifier = None
candidate_labels = ["rigore e autoritarismo", "libertà e apertura"]

def get_classifier():
    global classifier
    if classifier is None:
        device = 0 if torch.cuda.is_available() else -1
        classifier = pipeline("zero-shot-classification", model="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli", device=device)
    return classifier

def analyze_text_content(title, description, full_text):
    """
    Analizza il full_text se disponibile, altrimenti fallback su titolo.
    Non fa scraping, usa solo la CPU per calcoli.
    """

    if full_text and len(full_text) > 100:
        text_to_analyze = full_text[:1500]
        source_type = "FULL_TEXT"
    else:
        text_to_analyze = f"{title}. {description}"
        source_type = "METADATA"

    if len(text_to_analyze) < 10:
        return json.dumps({"verdict": "NEUTRAL", "ml_score": 0.5})

    try:
        model = get_classifier()
        res = model(text_to_analyze, candidate_labels, multi_label=False)

        idx = res['labels'].index("rigore e autoritarismo")
        score = float(res['scores'][idx])

        threshold = 0.60 if source_type == "FULL_TEXT" else 0.55
        verdict = "DUCE" if score > threshold else "NON DUCE"

        return json.dumps({
            "ml_label": res['labels'][0],
            "ml_score": score,
            "verdict": verdict,
            "method": source_type
        })
    except Exception:
        return json.dumps({"verdict": "ERROR", "ml_score": 0.0})

analyze_udf = udf(analyze_text_content, StringType())


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load() \
    .repartition(2)

df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))

df_analyzed = df_parsed.select("data.*") \
    .withColumn("analysis_json", analyze_udf(col("title"), col("description"), col("full_text")))

analysis_schema = StructType() \
    .add("ml_label", StringType()) \
    .add("ml_score", FloatType()) \
    .add("verdict", StringType()) \
    .add("method", StringType())

df_final = df_analyzed \
    .withColumn("analysis", from_json(col("analysis_json"), analysis_schema)) \
    .select(
        col("id"),
        col("query"),
        col("title"),
        col("url"),
        col("source"),
        col("publishedAt"),
        col("analysis")
    )

query = df_final.select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", OUTPUT_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()