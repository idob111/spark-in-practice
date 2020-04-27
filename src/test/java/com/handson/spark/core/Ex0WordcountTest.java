package com.handson.spark.core;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.*;

import static org.junit.Assert.*;


public class Ex0WordcountTest {

    private Ex0Wordcount ex0Wordcount;

    @Before
    public void init() {
        ex0Wordcount = new Ex0Wordcount();
    }

    @Test
    public void loadData() {
        // run
        Dataset<Row> wordCount = ex0Wordcount.wordDataset();

        // assert
        // this test is already green but see how we download the data in the loadData method
        assertEquals(809L, wordCount.count(), 0);
    }

    @Test
    public void wordcount() {
        assertEquals(381L, ex0Wordcount.wordCount(), 0);
    }

    @Test
    public void filterOnWordcount() {
        // run
        Dataset<Row> filtered = ex0Wordcount.filterOnWordCount();

        // assert
        assertEquals(26, filtered.count());
    }
}