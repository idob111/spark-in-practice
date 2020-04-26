package com.handson.spark.core;

import com.handson.spark.utils.Parse;
import com.handson.spark.utils.Tweet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  We still use the dataset with the 8198 reduced tweets. The data are reduced tweets as the example below:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the users:
 *  - find all the tweets by user
 *  - find how many tweets each user has
 *
 *  Use the Ex1UserMiningTest to implement the code.
 *
 */
public class Ex1UserMining {

    private static String pathToFile = "data/reduced-tweets.json";

    /**
     *  Load the data from the json file and return an RDD of Tweet
     */
    private Dataset<Row> loadData() {
      // Create spark configuration and spark context

        SparkSession conf = new SparkSession
                .Builder()
                .appName("Tweet")
                .master("local[*]").getOrCreate(); // here local mode. And * means you will use as much as you have cores.

        Dataset<Row> tweets=conf.read().json(pathToFile);

        return tweets;
    }

    /**
     *   For each user return all his tweets
     */
    public Dataset<Row> tweetsByUser() {
      Dataset<Row> tweets = loadData();

      Dataset<Row> tweetsByUser=tweets.groupBy(col("user")).agg(collect_list(col("text")));

      return tweetsByUser;
    }

    /**
     *  Compute the number of tweets by user
     */
    public Dataset<Row> tweetByUserNumber() {
      Dataset<Row> tweets = loadData();

      Dataset<Row> tweetsByUser=tweets.groupBy(col("user")).count();

      return tweetsByUser;
    }

}
