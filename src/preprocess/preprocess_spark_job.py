import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from pyspark.sql import SparkSession


if __name__ == '__main__':

    # remove negating words
    STOP_WORDS.difference_update(set(['no', 'not', 'dont']))

    spark = SparkSession\
            .builder\
            .appName("LemmatizeStopword")\
            .getOrCreate()


    ##### read data #######

    # File location and type
    file_location = "train_cleaned.csv"
    file_type = "csv"

    # CSV options
    infer_schema = True
    first_row_is_header = True
    delimiter = ","

    # The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(file_location)


    ######## Declaring the UDFs  ################
    # declare UDF for removing STOP WORDS
    remove_stop_word = udf(lambda document: ' '.join([token for token in str(document).split() if token not in STOP_WORDS]))

    def get_lemmas(document):

      document = str(document)
      doc = nlp(document)
      lemmas = []

      for token in doc:
          lemmas.append(token.lemma_)

      return ' '.join(lemmas)

    # UDF for lemmatization
    lemmatize_tokens = udf(get_lemmas)

    # Stop Word removal and Lemmatization
    # It is possible to chain the withColumn methods
    df = df.withColumn("message_new", remove_stop_word(df.message))\
           .withColumn("message_new", lemmatize_tokens(df.message))

    # write back to disk
    df.write.mode("overwrite")\
        .options(header=True, delimiter=',')\
        .csv("train_clean_lemmatized_stopremoved.csv")

    spark.stop()
