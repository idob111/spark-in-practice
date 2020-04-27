package com.handson.spark.core;

import com.handson.spark.utils.LoadJsonData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

/**
 * The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 * <p>
 * We still use the dataset with the 8198 reduced tweets. Here an example of a tweet:
 * <p>
 * {"id":"572692378957430785",
 * "user":"Srkian_nishu :)",
 * "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 * "place":"Orissa",
 * "country":"India"}
 * <p>
 * We want to make some computations on the tweets:
 * - Find all the persons mentioned on tweets
 * - Count how many times each person is mentioned
 * - Find the 10 most mentioned persons by descending order
 * <p>
 * Use the Ex2TweetMiningTest to implement the code.
 */
public class Ex2TweetMining {

    private String pathToFile = "data/reduced-tweets.json";


    /**
     * Find all the persons mentioned on tweets (case sensitive)
     */
    public Dataset<Row> mentionOnTweet() {
        Dataset<Row> tweets = LoadJsonData.loadData(pathToFile);

        Dataset<String> tweetsText = tweets.select(col("text")).as(Encoders.STRING());
        Dataset<Row> mention = tweetsText.flatMap(tweet -> Arrays.asList(tweet.split(" ")).iterator(), Encoders.STRING())
                .filter(word -> word.startsWith("@") && word.length() > 1).toDF("mentions");

        return mention;

    }

    /**
     * Count how many times each person is mentioned
     */
    public Dataset<Row> countMentions() {
        Dataset<Row> mentions = mentionOnTweet();

        Dataset<Row> mentionCount = mentions.groupBy(col("mentions")).count();

        return mentionCount;
    }

    /**
     * Find the 10 most mentioned persons by descending order
     */
    public Dataset<Row> top10mentions() {

        Dataset<Row> mostMentioned = countMentions().sort(desc("count")).limit(10);

        return mostMentioned;
    }

}
