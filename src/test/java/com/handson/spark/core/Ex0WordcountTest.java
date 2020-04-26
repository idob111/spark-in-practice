package com.handson.spark.core;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
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
    Long wordCount = ex0Wordcount.wordCount();

    // assert
    // this test is already green but see how we download the data in the loadData method
    assertEquals(809L, wordCount,0);
  }

  @Test
  public void wordcount() {
    // run
    JavaPairRDD<String, Integer> couples = ex0Wordcount.wordcount();

    // assert
    assertEquals(381, couples.count());
  }

  @Test
  public void filterOnWordcount() {
    // run
    JavaPairRDD<String, Integer> filtered = ex0Wordcount.filterOnWordcount();

    // assert
    assertEquals(26, filtered.count());
  }
}