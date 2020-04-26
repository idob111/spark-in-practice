package com.handson.spark.core;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 * <p>
 * Here the goal is to count how much each word appears in a file and make some operation on the result.
 * We use the mapreduce pattern to do this:
 * <p>
 * step 1, the mapper:
 * - we attribute 1 to each word. And we obtain then couples (word, 1), where word is the key.
 * <p>
 * step 2, the reducer:
 * - for each key (=word), the values are added and we will obtain the total amount.
 * <p>
 * Use the Ex0WordcountTest to implement the code.
 */
public class Ex0Wordcount {

    private static String pathToFile = "data/wordcount.txt";

    /**
     * Load the data from the text file and return an Dataset of words
     */
    private Dataset<String> loadData() {

        SparkSession conf = new SparkSession
                .Builder()
                .appName("Wordcount")
                .master("local[*]").getOrCreate(); // here local mode. And * means you will use as much as you have cores.


        return conf.read().textFile(pathToFile);

    }

    private Dataset<String> wordDataset() {
        return loadData().as(Encoders.STRING())
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING());
    }

    public Long wordCount() {
        return wordDataset().count();

    }

    /**
     * Now keep the word which appear strictly more than 4 times!
     */
    public Dataset<Row> filterOnWordCount() {

       return wordDataset().groupBy("value") //<k, iter(V)>
                .count()
                .toDF("word","count").sort(desc("count"));

    }

}
