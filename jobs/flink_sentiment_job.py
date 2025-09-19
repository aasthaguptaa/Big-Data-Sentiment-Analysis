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
    text = comment.get("body", "")
    cleaned = preprocessing(text)
    X    = vectorizer.transform([cleaned])
    prediction = float(model_logreg.predict_proba(X)[0, 1])
    # output: original all 7 features + sentiment score
    output = {**comment, "sentiment": prediction}
    return json.dumps(output)


env = StreamExecutionEnvironment.get_execution_environment()

consumer = FlinkKafkaConsumer(
    topics="reddit-sentiment-analysis",
    deserialization_schema=SimpleStringSchema(),
    properties= {
    "bootstrap.servers": "broker:9092",
    "group.id": "flink-sentiment-group",
    "auto.offset.reset": "earliest"
    }
)


producer = FlinkKafkaProducer(
    topic="reddit-sentiment-results",
    serialization_schema=SimpleStringSchema(),
    producer_config={"bootstrap.servers":"broker:9092"}
)


stream = env.add_source(consumer) \
            .map(extract_sentiment, output_type=Types.STRING())


stream.print()
stream.add_sink(producer)

env.execute("Reddit Sentiment Analysis Streaming")