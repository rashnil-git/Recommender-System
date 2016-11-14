package data;

import breeze.optimize.linear.LinearProgram;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;
import java.lang.*;
import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.Tuple2$;

/**
 * Created by Rash on 04-11-2016.
 */
public class ModelBuilding implements Serializable{

    //private JavaPairRDD<Long,Ratings> ratings_rdd;
    private JavaRDD<Rating> spark_ratings_rdd;
    private JavaRDD<Rating> training_set_rdd;
    private JavaRDD<Rating> test_set_rdd;
    private MatrixFactorizationModel model;

    public ModelBuilding(JavaRDD<Rating> ratings_rdd) {

       /* JavaRDD<Rating> ml_rating_rdd = ratings_rdd.map(new Function<Tuple2<Long, Ratings>, Rating>() {
            @Override
            public Rating call(Tuple2<Long, Ratings> tupleRating) throws Exception {


                return new Rating((int) tupleRating._2().getUserID(), (int) tupleRating._2().getMovieID(), tupleRating._2().getRatings());
            }
        });*/

        //Assign to spark ratings rdd object and cache in this rdd.
        this.spark_ratings_rdd = ratings_rdd;
        this.spark_ratings_rdd.cache();

    }


    public void buildModel(int rank,int numIterations,double regFactor)
    {
        //set the fraction for test and training set.
        double test_fraction=0.4;
        double test_frac[]={1-test_fraction,test_fraction};

        //split the ratings rdd into training and test based on test fraction.
        JavaRDD<Rating>[] split_rdds =this.spark_ratings_rdd.randomSplit(test_frac);

        //assign the respective partition to the test/train rdd
        this.training_set_rdd =split_rdds[0];
        this.test_set_rdd =split_rdds[1];

        //build model using training data
        MatrixFactorizationModel _model = ALS.train(this.training_set_rdd.rdd(),rank,numIterations,regFactor);

        this.model = _model;


    }


    public double modelEvaluation()
    {   //Variable to store Mean Square Error
        double MSE=0;

        //create userMovie RDD using the test data set. This RDD contains just the user id and movie id.
        JavaRDD<Tuple2<Object,Object>> userMovies =this.test_set_rdd.map(new Function<Rating, Tuple2<Object, Object>>() {
            @Override
            public Tuple2<Object, Object> call(Rating rating) throws Exception {
                return new Tuple2<Object, Object>(rating.user(),rating.product());
            }
        });

        //Predict the ratings for user id and movie id combination from the test set using our model.
        JavaPairRDD<Tuple2<Integer,Integer>,Double> predicted_rdd=JavaPairRDD.fromJavaRDD(
                this.model.predict(JavaRDD.toRDD(userMovies)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer,Integer>,Double>>() {
                    public Tuple2<Tuple2<Integer,Integer>,Double> call(Rating rating){
                        return new Tuple2<Tuple2<Integer,Integer>,Double>
                                (new Tuple2<Integer,Integer>(rating.user(),rating.product()),rating.rating());
                    }
                }));

        //create RDD with Actual and Predicted Ratings for the same user ID and Movie ID in Test data set.
        JavaRDD<Tuple2<Double,Double>> predict_rating_rdd =JavaPairRDD.fromJavaRDD(this.test_set_rdd.map
                (new Function<Rating, Tuple2<Tuple2<Integer,Integer>,Double>>() {
            @Override
            public Tuple2<Tuple2<Integer,Integer>,Double> call(Rating rating){
                return new Tuple2<Tuple2<Integer,Integer>,Double>
                        (new Tuple2<Integer,Integer>(rating.user(),rating.product()),rating.rating());
            }
        })).join(predicted_rdd).values();


        //Calculate the mean sqaure error comparing the predicted ratings vs the actual ratings.
         MSE = JavaDoubleRDD.fromRDD(predict_rating_rdd.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();

        return Math.sqrt(MSE);

    }

    public JavaRDD<Tuple2<Object,Rating[]>> recommendMovies(final Integer user_id) {


        JavaRDD<Tuple2<Object,Rating[]>> recommendations= this.model.recommendProductsForUsers(10).toJavaRDD();




        JavaRDD<Tuple2<Object,Rating[]>> filtered_recommendations=recommendations.filter(new Function<Tuple2<Object, Rating[]>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Object, Rating[]> recommend) throws Exception {
                    return (recommend._1()==user_id);

            }
        });


        return filtered_recommendations;


       /* JavaPairRDD<Tuple2<Integer,Integer>,Double> predicted_rdd=JavaPairRDD.fromJavaRDD(
                this.model.predict(JavaRDD.toRDD(user_rating_rdd)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer,Integer>,Double>>() {
                            public Tuple2<Tuple2<Integer,Integer>,Double> call(Rating rating){
                                return new Tuple2<Tuple2<Integer,Integer>,Double>
                                        (new Tuple2<Integer,Integer>(rating.user(),rating.product()),rating.rating());
                            }
                        }));*/



    }



   /* public void recommendMovies()
    {
        //creaing rdd with movie id and rating object.
        JavaPairRDD<Integer,Rating> movie_ratings=this.spark_ratings_rdd.mapToPair(new PairFunction<Rating, Integer, Rating>() {
            @Override
            public Tuple2<Integer,Rating> call(Rating rating) throws Exception {
                return new Tuple2<Integer,Rating>(rating.product(),rating);
            }
        });

      //  JavaPairRDD<Integer,Iterable<Rating>> movie_grouped_ratings =movie_ratings.groupByKey();

        //calculate sum of ratings for each movie
        JavaPairRDD<Integer,Double> movie_ratings_sum =movie_ratings.mapToPair(new PairFunction<Tuple2<Integer, Rating>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Rating> integerRatingTuple2) throws Exception {
                return new Tuple2<Integer, Double>(integerRatingTuple2._1(),integerRatingTuple2._2().rating());
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double a1, Double a2) throws Exception {
                return a1+a2;
            }
        });

        System.out.println("Sum :"+movie_ratings_sum.take(10));

        //Get the number of ratings received for each movie.
        JavaPairRDD<Integer,Integer> movie_ratings_count =movie_ratings.mapToPair(new PairFunction<Tuple2<Integer, Rating>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Rating> integerRatingTuple2) throws Exception {
                return new Tuple2<Integer, Integer>(integerRatingTuple2._1(), Integer.valueOf(1));
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        System.out.println("Count :"+movie_ratings_count.take(10));

        //Join the two rdd into one
        JavaPairRDD<Integer,Tuple2<Double,Integer>> movie_combined_ratings=movie_ratings_sum.join(movie_ratings_count);

        //Form RDD with movie and average rating.
        JavaPairRDD<Integer,Double> movie_avg_rating =movie_combined_ratings.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Integer>> integerTuple2Tuple2) throws Exception {

                Double avg_rating=Double.valueOf(String.format("%.1f",integerTuple2Tuple2._2._1/integerTuple2Tuple2._2._2));

                return new Tuple2<Integer, Double>(integerTuple2Tuple2._1,avg_rating);
            }
        });

        Form RDD with movie and average rating.
        //JavaPairRDD<Integer,Tuple2<Double,Integer>> movie_avg_rating =movie_combined_ratings.
                mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>, Integer, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<Integer, Tuple2<Double, Integer>> call(Tuple2<Integer, Tuple2<Double, Integer>> tuple) throws Exception {
                Double avg_rating=Double.valueOf(String.format("%.1f",tuple._2._1/tuple._2._2));

                return new Tuple2<Integer, Tuple2<Double, Integer>>(tuple._1,new Tuple2<Double,Integer>(avg_rating,tuple._2._2));
            }
        });


           // movie_avg_rating.
                System.out.println("Average Rating :" + movie_avg_rating.take(10));

    }


    public void saveModel(Application as)
    {
        this.model.save(as.getSparkContext().sc(), "model/CollaborativeFilters/");

    }

    private void loadModel(Application as)
    {
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(as.getSparkContext().sc(),
                "model/CollaborativeFilters/");

        this.model= sameModel;
    }*/


}







