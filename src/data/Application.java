package data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Rash on 29-10-2016.
 * Initialize Spark Context and Initialize Application
 */

public class Application {

private JavaSparkContext sparkContext=null;


    public Application() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Recommender").set("spark.executor.memory", "4g")
                .set("spark.network.timeout","1000").set("spark.executor.heartbeatInterval","100");

        this.sparkContext = new JavaSparkContext(conf);
    }

    public JavaSparkContext getSparkContext() {
        return this.sparkContext;
    }
}
