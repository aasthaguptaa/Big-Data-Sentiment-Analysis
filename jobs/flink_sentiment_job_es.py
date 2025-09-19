import json
import pickle
import os
import emoji
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.java_gateway import get_gateway
from pyflink.datastream.connectors.elasticsearch import ElasticsearchEmitter,ElasticsearchSinkBuilderBase, Elasticsearch7SinkBuilder, ElasticsearchSink, FlushBackoffType
from pyflink.datastream.connectors import Sink, DeliveryGuarantee
from datetime import datetime, timezone
from pyflink.datastream.functions import MapFunction



nltk.download("punkt") # tokenizer
nltk.download("stopwords")


# open the model and vectorizer from pickle files
INPUT_DIR = "models"
with open(os.path.join(INPUT_DIR, "model_logistic_regression.pkl"), "rb") as model_file:
    model_logreg = pickle.load(model_file)
with open(os.path.join(INPUT_DIR, "tfidf_vectorizer.pkl"), "rb") as vectorizer_file:
    vectorizer = pickle.load(vectorizer_file)


# same preprocessing like offline model training
stop_words = set(stopwords.words("english"))
tokenizer = RegexpTokenizer(r"<[^>]+>|[A-Za-z]+")


def preprocessing(text):
    # convert emojis to text in <> brackets
    text = emoji.demojize(text, delimiters=("<", ">"))
    # convert every word to lowercase to avoid duplicates (Happy and happy should be same)
    text = text.lower()
    # tokenizing
    tokens = tokenizer.tokenize(text)
    tokens = [t for t in tokens if t not in stop_words]
    return " ".join(tokens)


def extract_sentiment(comment_json):
    comment = json.loads(comment_json)
    ts = comment.get("created_utc")
    try:
        # converting timestamp into integer 
        comment["created_utc"] = int(ts)
    except Exception:
        # if converting fails, use timestamp
        comment["created_utc"] = int(datetime.now(tz=timezone.utc).timestamp())
    
    # ingest_time can be ISO format since it's mapped as date
    comment["ingest_time"] = datetime.now(tz=timezone.utc).isoformat()
    
    # Extract sentiment
    text = comment.get("body", "")
    cleaned = preprocessing(text)
    X = vectorizer.transform([cleaned])
    prediction = float(model_logreg.predict_proba(X)[0, 1])
    
    output = {**comment, "sentiment": prediction}
    return json.dumps(output)

def to_string_map(record: str) -> dict:
    """Convert values for Elasticsearch, keeping sentiment as float"""
    data = json.loads(record)
    # convert all the variables into string except sentiment
    string_map = {}
    for k, v in data.items():
        if k == "sentiment":
            # Elasticsearch will turn into float
            string_map[k] = str(v)
        elif isinstance(v, (dict, list)):
            string_map[k] = json.dumps(v)
        else:
            string_map[k] = str(v)
    return string_map


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(10)
env.enable_checkpointing(5000)



consumer = FlinkKafkaConsumer(
    topics="reddit-sentiment-analysis",
    deserialization_schema=SimpleStringSchema(),
    properties= {
    "bootstrap.servers": "broker:9092",
    "group.id": "flink-sentiment-group",
    "auto.offset.reset": "earliest",
    "stop-on-checkpoint": "true" 
    }
)


producer = FlinkKafkaProducer(
    topic="reddit-sentiment-results",
    serialization_schema=SimpleStringSchema(),
    producer_config={"bootstrap.servers":"broker:9092"}
)

stream = env.add_source(consumer).map(extract_sentiment, output_type=Types.STRING())
stream.add_sink(producer)
stream.print()

'''def to_string_map(record: str) -> dict:
    data = json.loads(record)
    # Convert all values to string for safety
    return {k: json.dumps(v) if isinstance(v, (dict,list)) else str(v) for k, v in data.items()}
'''

# Main Elasticsearch Builder
es_stream = stream.map(to_string_map, output_type=Types.MAP(Types.STRING(), Types.STRING()))
es_sink = (
    Elasticsearch7SinkBuilder()
    # If we don't use key_id th Elasticsearch will automatically create
    .set_emitter(ElasticsearchEmitter.static_index("sentiment_analysis"))
        .set_hosts(['http://elasticsearch:9200'])
    .set_bulk_flush_max_actions(100)  
    .set_bulk_flush_interval(5000)     
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)  
    .build()
)

es_stream.sink_to(es_sink)

env.execute("Reddit Sentiment Analysis Streaming")