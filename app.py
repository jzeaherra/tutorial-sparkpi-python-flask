import os

from flask import Flask
from flask import request
from pyspark.sql import SparkSession


app = Flask(__name__)


def letterCount(word):
    spark = SparkSession.builder.appName("LetterCount").getOrCreate()

    myList = list(word)
    count = spark.sparkContext.parallelize(
        myList).map(lambda letter: [letter , 1] ).reduceByKey(lambda x, y: x + y)
    result = count.collect()
    spark.stop()
    return result


@app.route("/")
def index():
    return "Python Flask LetterCount server running. Add the 'lettercount?word=xxx' route to this URL to invoke the app."


@app.route("/lettercount")
def lettercount():
    word = str(request.args.get('word'))
    count = letterCount(word)
    response = "Letter count: {}".format(count)

    return response


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
