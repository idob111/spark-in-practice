package com.handson.spark.utils;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Data
@NoArgsConstructor
public class LoadJsonData {

    public static Dataset<Row> loadData(String pathToFile) {
        // Create spark configuration and spark context

        SparkSession conf = new SparkSession
                .Builder()
                .appName("Tweet")
                .master("local[*]").getOrCreate(); // here local mode. And * means you will use as much as you have cores.

        return conf.read().json(pathToFile);
    }
}
