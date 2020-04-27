package com.handson.spark.core;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.spark.sql.functions.col;

public class Ex2TweetMiningTest {

    private Ex2TweetMining ex2TweetMining;

    @Before
    public void init() {
        ex2TweetMining = new Ex2TweetMining();
    }

    @Test
    public void mentionOnTweet() {
        // run
        Dataset<Row> mentions = ex2TweetMining.mentionOnTweet();

        // assert
        Assert.assertEquals(4462, mentions.count());
        Dataset<Row> filter = mentions.where(col("mentions").equalTo("@JordinSparks"));
        Assert.assertEquals(2, filter.count());
    }

    @Test
    public void countMentions() {
        // run
        Dataset<Row> counts = ex2TweetMining.countMentions();

        // assert
        Assert.assertEquals(3283, counts.count());
        Dataset<Row> filter = counts.where(col("mentions").equalTo("@JordinSparks"));
        Assert.assertEquals(1, filter.count());
        Assert.assertEquals(2, filter.first().getLong(1));
    }

    @Test
    public void top10mentions() {
        // run
        Dataset<Row> mostMentioned = ex2TweetMining.top10mentions();

        // assert
        Assert.assertEquals(10, mostMentioned.count());
        Assert.assertEquals(189L, mostMentioned.first().getLong(1));
        Assert.assertEquals("@ShawnMendes", mostMentioned.first().getString(0));
        Assert.assertEquals(100L, mostMentioned.collectAsList().get(1).getLong(1));
        Assert.assertEquals("@HIITMANonDECK", mostMentioned.collectAsList().get(1).getString(0));
    }

}