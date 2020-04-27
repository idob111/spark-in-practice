package com.handson.spark.core;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.spark.sql.functions.col;

public class Ex4InvertedIndexTest {

  private Ex4InvertedIndex ex4InvertedIndex;

  @Before
  public void init() {
    ex4InvertedIndex = new Ex4InvertedIndex();
  }

  @Test
  public void invertedIndex() {
    // run
    Dataset<Row> map = ex4InvertedIndex.invertedIndex();

    // assert
    Assert.assertEquals(2461, map.count());
    Dataset<Row> paris = map.where(col("mentions").equalTo("#paris"));
    Assert.assertEquals(54, paris.first().getInt(2));
    Dataset<Row> edc = map.where(col("mentions").equalTo("#EDC"));
    Assert.assertEquals(8, edc.first().getInt(2));
  }
}