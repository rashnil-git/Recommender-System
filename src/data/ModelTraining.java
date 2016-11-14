package data;

import data.Ratings;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by Rash on 13-11-2016.
 */
public class ModelTraining implements Serializable {

    private JavaRDD<Rating> train_rating_rdd;
    private JavaRDD<Rating> training_set_rdd;
    private JavaRDD<Rating> test_set_rdd;

    public ModelTraining(JavaPairRDD<Long, Ratings> ratings_rdd) {

        JavaRDD<Rating> ml_rating_rdd = ratings_rdd.map(new Function<Tuple2<Long, Ratings>, Rating>() {
            @Override
            public Rating call(Tuple2<Long, Ratings> tupleRating) throws Exception {


                return new Rating((int) tupleRating._2().getUserID(), (int) tupleRating._2().getMovieID(), tupleRating._2().getRatings());
            }
        });

        //Assign to spark ratings rdd object and cache in this rdd.
        this.train_rating_rdd = ml_rating_rdd;
       // this.train_rating_rdd.cache();

    }

    public String returnBestRank(int [] rank){

        double test_fraction=0.4;
        double test_frac[]={1-test_fraction,test_fraction};
        double RMSE=1.0;
        double MSE=0;
        int _rank=0;
        //split the ratings rdd into training and test based on test fraction.


        //assign the respective partition to the test/train rdd
        //this.training_set_rdd =split_rdds[0];
        //this.test_set_rdd =split_rdds[1];

        for(int rank_i=0;rank_i<rank.length;rank_i++) {

            //split the ratings rdd into training and test based on test fraction.
            JavaRDD<Rating>[] split_rdds =this.train_rating_rdd.randomSplit(test_frac);

            //assign the respective partition to the test/train rdd
            this.training_set_rdd =split_rdds[0];
            this.test_set_rdd =split_rdds[1];


            //build model using training data
            MatrixFactorizationModel _model = ALS.train(this.training_set_rdd.rdd(),rank[rank_i], 10, 0.1);

            //create userMovie RDD using the test data set. This RDD contains just the user id and movie id.
            JavaRDD<Tuple2<Object,Object>> userMovies =this.test_set_rdd.map(new Function<Rating, Tuple2<Object, Object>>() {
                @Override
                public Tuple2<Object, Object> call(Rating rating) throws Exception {
                    return new Tuple2<Object, Object>(rating.user(),rating.product());
                }
            });

            JavaPairRDD<Tuple2<Integer,Integer>,Double> predicted_rdd=JavaPairRDD.fromJavaRDD(
                    _model.predict(JavaRDD.toRDD(userMovies)).toJavaRDD().map(
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

            if(RMSE>Math.sqrt(MSE)) {
                RMSE = Math.sqrt(MSE);
                _rank=rank[rank_i];
            }
        }
        String return_scores=RMSE+","+_rank;

        return return_scores;
    }

/*    public void returnBestLambda()
    {

    }*/

}
