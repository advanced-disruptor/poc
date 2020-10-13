# pyspark_job.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    return SparkSession.builder.config(
	"spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()

def process_book_data(spark, input_path, output_path):
    df = spark.read.parquet(input_path)
    book_agg = df.filter(df.verified_purchase == 'Y') \
        .groupBy('product_title') \
        .agg({'star_rating': 'avg', 'review_id': 'count'}) \
        .filter(F.col('count(review_id)') >= 500) \
        .sort(F.desc('avg(star_rating)')) \
        .select(F.col('product_title').alias('book_title'),
                F.col('count(review_id)').alias('review_count'),
                F.col('avg(star_rating)').alias('review_avg_stars'))
    book_agg.write.mode('overwrite').save(output_path)

def main():
    spark = create_spark_session()
    input_path = ('s3://amazon-reviews-pds/parquet/' +
                  'product_category=Books/*.parquet')
    output_path = 's3://pyspark-aws-emr/result'
    process_book_data(spark, input_path, output_path)

if __name__ == '__main__':
    main()

