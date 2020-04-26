import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class test {
    private static String pathToFile = "data/wordcount.txt";

    public static void main(String [] args)
    {

        /**
         *  Load the data from the text file and return an RDD of words
         */
        // create spark configuration and spark context: the Spark context is the entry point in Spark.
        // It represents the connexion to Spark and it is the place where you can configure the common properties
        // like the app name, the master url, memories allocation...
        SparkSession conf = new SparkSession
                .Builder()
                .appName("Wordcount")
                .master("local[*]").getOrCreate(); // here local mode. And * means you will use as much as you have cores.


        // load data and create an RDD where each element will be a word
        // Here the flatMap method is used to separate the word in each line using the space separator
        // In this way it returns an RDD where each "element" is a word
        Dataset<Row> words = conf.read().json(pathToFile);

        words.show();

    }

}
