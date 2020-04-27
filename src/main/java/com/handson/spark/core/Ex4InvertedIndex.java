package com.handson.spark.core;

import com.handson.spark.utils.LoadJsonData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * Buildind a hashtag search engine
 * <p>
 * The goal is to build an inverted index. An inverted index is the data structure used to build search engines.
 * <p>
 * How does it work?
 * <p>
 * Assuming #spark is an hashtag that appears in tweet1, tweet3, tweet39.
 * The inverted index that you must return should be a Map (or HashMap) that contains a (key, value) pair as (#spark, List(tweet1,tweet3, tweet39)).
 */
public class Ex4InvertedIndex {

    private static String pathToFile = "data/reduced-tweets.json";


    public Dataset<Row> invertedIndex() {
        Dataset<Row> tweets = LoadJsonData.loadData(pathToFile);

        Dataset<String> tweetsText = tweets.select(col("text")).as(Encoders.STRING());
        Dataset<Row> mentions = tweetsText.flatMap(tweet -> Arrays.asList(tweet.split(" ")).iterator(), Encoders.STRING())
                .filter(word -> word.startsWith("#") && word.length() > 1).dropDuplicates().toDF("mentions");

        Dataset<Row> tw = mentions.join(tweetsText, (tweetsText.col("text")).contains((mentions.col("mentions"))));

        Dataset<Row> map = tw.groupBy(col("mentions")).agg(collect_list(col("text")).alias("tweets"))
                .withColumn("size", size(col("tweets")));

        return map;
    }

}
