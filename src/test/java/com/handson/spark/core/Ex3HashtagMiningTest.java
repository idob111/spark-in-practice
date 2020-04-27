package com.handson.spark.core;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.spark.sql.functions.col;

public class Ex3HashtagMiningTest {

  private Ex3HashtagMining ex3HashtagMining;

  @Before
  public void init() {
    ex3HashtagMining = new Ex3HashtagMining();
  }

  @Test
  public void mentionOnTweet() {
    // run
    Dataset<Row> mentions = ex3HashtagMining.hashtagMentionedOnTweet();

    // assert
    Assert.assertEquals(5262, mentions.count());
    Dataset<Row> filter = mentions.where(col("mentions").equalTo("#youtube"));
    Assert.assertEquals(2, filter.count());
  }

  @Test
  public void countMentions() {
    // run
    Dataset<Row> counts = ex3HashtagMining.countMentions();

    // assert
    Assert.assertEquals(2461, counts.count());
    Dataset<Row> filter = counts.filter(col("mentions").equalTo("#youtube"));
    Assert.assertEquals(1, filter.count());
    Assert.assertEquals(2L, filter.first().getLong(1));
  }

  @Test
  public void mostMentioned() {
    // run
    Dataset<Row> mostMentioned = ex3HashtagMining.top10HashTags();

    // assert
    Assert.assertEquals(10, mostMentioned.count());
    Assert.assertEquals(253, mostMentioned.first().getLong(1));
    Assert.assertEquals("#DME", mostMentioned.first().getString(0));
  }

}