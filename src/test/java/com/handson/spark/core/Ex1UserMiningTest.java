package com.handson.spark.core;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.spark.sql.functions.col;


public class Ex1UserMiningTest {

    private Ex1UserMining ex1UserMining;

    @Before
    public void init() {
        ex1UserMining = new Ex1UserMining();
    }

    @Test
    public void tweetsByUser() {
        // run
        Dataset<Row> tweetsByUser = ex1UserMining.tweetsByUser();

        // assert
        Assert.assertEquals(5967, tweetsByUser.count());
    }

    @Test
    public void tweetByUserNumber() {
        // run
        Dataset<Row> result = ex1UserMining.tweetByUserNumber();

        // assert
        Assert.assertEquals(5967, result.count());

        Dataset<Row> example = result.where(col("user").equalTo("Dell Feddi"));
        Assert.assertEquals(1, example.count());
        Assert.assertEquals(29, example.first().getLong(1));

    }

}
