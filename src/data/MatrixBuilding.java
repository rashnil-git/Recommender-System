package data;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;
import java.lang.*;
import scala.Tuple2;

/**
 * Created by Rash on 04-11-2016.
 */
public class MatrixBuilding implements Serializable{

    private JavaPairRDD<Long,Ratings> ratings_rdd;
    private JavaRDD<Rating> spark_ratings_rdd;
    private MatrixFactorizationModel model;

    public MatrixBuilding(JavaPairRDD<Long, Ratings> ratings_rdd) {
        this.ratings_rdd = ratings_rdd;
    }

     //create Spark built in Rating RDD
    public JavaRDD<Rating> loadRatingRDD() {
        JavaRDD<Rating> spark_rating_rdd = this.ratings_rdd.map(new Function<Tuple2<Long, Ratings>, Rating>() {
            @Override
            public Rating call(Tuple2<Long, Ratings> tupleRating) throws Exception {


                return new Rating((int) tupleRating._2().getUserID(), (int) tupleRating._2().getMovieID(), tupleRating._2().getRatings());
            }
        });

        return spark_rating_rdd;
    }


    public void setSpark_ratings_rdd(JavaRDD<Rating> spark_ratings_rdd) {
        this.spark_ratings_rdd = spark_ratings_rdd;
    }


    public void buildModel(int rank,int numIterations,double regFactor)
    {
        MatrixFactorizationModel _model = ALS.train(this.spark_ratings_rdd.rdd(),rank,numIterations,regFactor);

        this.model = _model;
    }


    public double modelEvaluation()
    {
        double MSE=0;

        JavaRDD<Tuple2<Object,Object>> userMovies =this.spark_ratings_rdd.map(new Function<Rating, Tuple2<Object, Object>>() {
            @Override
            public Tuple2<Object, Object> call(Rating rating) throws Exception {
                return new Tuple2<Object, Object>(rating.user(),rating.product());
            }
        });

        JavaPairRDD<Tuple2<Integer,Integer>,Double> predicted_rdd=JavaPairRDD.fromJavaRDD(
                this.model.predict(JavaRDD.toRDD(userMovies)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer,Integer>,Double>>() {
                    public Tuple2<Tuple2<Integer,Integer>,Double> call(Rating rating){
                        return new Tuple2<Tuple2<Integer,Integer>,Double>(new Tuple2<Integer,Integer>(rating.user(),rating.product()),rating.rating());
                    }
                }));

        JavaRDD<Tuple2<Double,Double>> predict_rating_rdd =JavaPairRDD.fromJavaRDD(this.spark_ratings_rdd.map(new Function<Rating, Tuple2<Tuple2<Integer,Integer>,Double>>() {
            @Override
            public Tuple2<Tuple2<Integer,Integer>,Double> call(Rating rating){
                return new Tuple2<Tuple2<Integer,Integer>,Double>(new Tuple2<Integer,Integer>(rating.user(),rating.product()),rating.rating());
            }
        })).join(predicted_rdd).values();


         MSE = JavaDoubleRDD.fromRDD(predict_rating_rdd.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();

        return MSE;

    }

}







