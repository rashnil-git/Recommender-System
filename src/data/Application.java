package data;

import org.apache.commons.configuration2.INIConfiguration;
import org.apache.commons.configuration2.SubnodeConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import javax.servlet.ServletContext;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rash on 29-10-2016.
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
