package data.loaders;

/**
 * Created by Rash on 29-10-2016.
 */

import com.sun.xml.bind.v2.TODO;
import data.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.sources.In;
import org.joda.time.LocalDateTime;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterable;
import spire.math.QuickSort;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/*
This program implements Recommender System logic using MovieLens Dataset.
 MovieLens 100k is used for training the MatrixFactorization Model
 MovieLens 22million ratings data set is used for testing and predictions.

This application is uses the Machine Learning Libraries of Apache Spark to build prediction model

*/


  public class MovieDataLoader implements Serializable{

      //Private variables that store the path to the data files.
      private String dataDirectory;
      private String movieData;
      private String ratingData;
      private String userTagsData;
      private String movieLinkData;


      //Constructor which assigns the values to the variables
      public MovieDataLoader(String dataDirectory, String movieLinkData, String userTagsData, String movieData, String ratingData) {
          this.dataDirectory = dataDirectory;
          this.movieLinkData = movieLinkData;
          this.userTagsData = userTagsData;
          this.movieData = movieData;
          this.ratingData = ratingData;
      }

      //Function to remove header from the file and return rdd with no header
      public Function2 _header= new Function2<Integer, Iterator<String>, Iterator<String>>() {
          @Override
          public Iterator<String> call(Integer i, Iterator<String> iterator) throws Exception {
              if (i == 0 && iterator.hasNext()) {
                  iterator.next();
                  return iterator;
              } else
                  return iterator;
          };
      };

      //load the movie data
        public JavaPairRDD<Integer, Movie> loadMovieData(Application as) throws IOException
        {
            Integer movieID;
            String movieName;
            Map<Integer,Movie> movie_map =new HashMap<Integer, Movie>();

            String movie_path=this.dataDirectory+File.separator+this.movieData;

            JavaRDD<String> movie_file_rdd= as.getSparkContext().textFile(movie_path);

            JavaRDD<String> movie_data_rdd= movie_file_rdd.mapPartitionsWithIndex(_header,false);

            List<String> movie_list =movie_data_rdd.collect();


            for(int movie_list_i=0;movie_list_i<movie_list.size();movie_list_i++)
            {
                String movie_list_string[] = movie_list.get(movie_list_i).split(",");

                movieID = Integer.parseInt(movie_list_string[0]);
                movieName = movie_list_string[1];

                String genres[] = movie_list_string[2].split("\\|");

                List<String> genres_list = Arrays.asList(genres);


                Movie _movie = new Movie(movieID, movieName, genres_list);

                movie_map.put(movieID,_movie);

            }

            ArrayList movie_map_list = new ArrayList(movie_map.entrySet());

            ArrayList<Tuple2<Integer,Movie>> movie_tuple_list = new ArrayList<Tuple2<Integer,Movie>>();
            Iterator<Map.Entry<Integer,Movie>> _iter = movie_map_list.iterator();
            while (_iter.hasNext()) {
                Map.Entry<Integer,Movie> _value = _iter.next();
                movie_tuple_list.add(new Tuple2<Integer,Movie>(_value.getKey(), _value.getValue()));
            }


            JavaPairRDD<Integer,Movie> movies_rdd =as.getSparkContext().parallelizePairs(movie_tuple_list);

            return movies_rdd;

        }

      //load the Ratings data
        public JavaPairRDD<Long,Ratings> loadRatingsData(Application as)
        {

            String ratings_path=this.dataDirectory+File.separator+this.ratingData;

            JavaRDD<String> ratings_file_rdd= as.getSparkContext().textFile(ratings_path);

            JavaRDD<String> ratings_data_rdd= ratings_file_rdd.mapPartitionsWithIndex(_header,false);


            JavaPairRDD<Long,Ratings> ratings_rdd  = ratings_data_rdd.mapToPair(new PairFunction<String, Long, Ratings>() {
                @Override
                public Tuple2<Long, Ratings> call(String s) throws Exception {
                    long movieID;

                    String rate_list_string[] = s.split(",");
                    movieID=Long.valueOf(rate_list_string[1]);
                    long userID= Long.valueOf(rate_list_string[0]);
                    double ratings=Double.valueOf(rate_list_string[2]);
                    long timestamp =Long.valueOf(rate_list_string[3]);

                   // ArrayList ratings_list =new ArrayList<Tuple3<Long, Double, Long>>();
                    //ratings_list.add(new Tuple3<Long, Double, Long>(userID,ratings,timestamp));


                    Ratings _ratings=new Ratings(movieID,userID,ratings,timestamp);

                    return (new Tuple2<Long,Ratings>(movieID,_ratings));
                }
            });

        //    JavaPairRDD<Long, java.lang.Iterable<Ratings>> final_ratings_rdd =ratings_rdd.groupByKey();

            System.out.println(ratings_rdd.count());

            //Filter Ratings data to consider movies rated 3.5 and above
            JavaPairRDD<Long,Ratings> filtered_rating_rdd=ratings_rdd.filter(new Function<Tuple2<Long, Ratings>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Long, Ratings> longRatingsTuple2) throws Exception {

                    if (longRatingsTuple2._2().getRatings() >= 3.5) {
                        return true;
                    } else
                    {
                        return false;
                    }

                }
            });

            System.out.println(filtered_rating_rdd.count());

            return filtered_rating_rdd;

            }

        //load user tag data for movies - comments from users
        public JavaPairRDD<Long,UserTags> loadUserTagData(Application as) throws IOException
            {

                /*TODO : Incorporate user comments to identify best movie*/

                long tagID,movieID;
                double timestamp;
                String tags;

                Map<Long,UserTags> tags_map =new HashMap<Long, UserTags>();
                ArrayList<String> tags_list=new ArrayList<String>();

                String tag_path=this.dataDirectory+File.separator+this.userTagsData;

                JavaRDD<String> tag_file_rdd= as.getSparkContext().textFile(tag_path);

                JavaRDD<String> tag_data_rdd= tag_file_rdd.mapPartitionsWithIndex(_header,false);

                List<String> tags_data =tag_data_rdd.collect();


                for(int tag_data_i=0;tag_data_i<tags_data.size();tag_data_i++)
                {
                    String tag_list_string[] = tags_data.get(tag_data_i).split(",");
                    int tag_size =tag_list_string.length;
                    tags_list=new ArrayList<String>();

                    try {
                       tagID = Long.valueOf(tag_list_string[0]);
                       movieID = Long.valueOf(tag_list_string[1]);
                       timestamp = Double.valueOf(tag_list_string[tag_size-1]);
                       String tag_line=tag_list_string[2];

                        if(tag_size>4)
                        { for (int extra_tags_i=3;extra_tags_i<=tag_size-2;extra_tags_i++)
                         {
                            tag_line= tag_line.concat(",").concat(tag_list_string[extra_tags_i]);
                         }
                        }

                       if(tags_map.containsKey(movieID))
                       {
                           tags_map.get(movieID).getTag().add(tag_line);

                       }else {

                           tags_list=new ArrayList<String>();
                           tags_list.add(tag_line);
                           UserTags _tags = new UserTags(tagID, movieID,tags_list,timestamp);
                           tags_map.put(movieID, _tags);
                       }

                   }catch(Exception e)
                   {
                        System.out.println("Exception Inside UserTags:"+e);
                   }


                }

                ArrayList tags_map_list = new ArrayList(tags_map.entrySet());

                ArrayList<Tuple2<Long,UserTags>> tag_tuple_list = new ArrayList<Tuple2<Long,UserTags>>();
                Iterator<Map.Entry<Long,UserTags>> _iter = tags_map_list.iterator();
                while (_iter.hasNext()) {
                    Map.Entry<Long,UserTags> _value = _iter.next();
                    tag_tuple_list.add(new Tuple2<Long,UserTags>(_value.getKey(), _value.getValue()));
                }


                JavaPairRDD<Long,UserTags> tags_rdd =as.getSparkContext().parallelizePairs(tag_tuple_list);

                return tags_rdd;

            }


        //load IMDB mapping data with that of MovieLens movie ID
        public JavaPairRDD<Long,Links> loadIMDBLinkData(Application as)
        {
            /*TODO : Can be used in future; Disabled for now*/

            long movieID;
            long imdbID;
           // long tmdbID;
            Map<Long,Links> link_map=new HashMap<Long, Links>();


            String links_path=this.dataDirectory+File.separator+this.movieLinkData;

            JavaRDD<String> link_file_rdd= as.getSparkContext().textFile(links_path);

            JavaRDD<String> link_data_rdd= link_file_rdd.mapPartitionsWithIndex(_header,false);

            List<String> link_data =link_data_rdd.collect();


            for(int link_data_i=0;link_data_i<link_data.size();link_data_i++)
            {
                String link_data_string []=link_data.get(link_data_i).split(",");

                try {
                    movieID = Long.valueOf(link_data_string[0]);
                    imdbID = Long.valueOf(link_data_string[1]);
                   // tmdbID = Long.valueOf(link_data_string[2]);

                    Links _links = new Links(movieID, imdbID);

                    link_map.put(movieID, _links);
                }catch (Exception e)
                {

                    System.out.println("Exception in Link:"+e);
                }

            }


            ArrayList links_map_list = new ArrayList(link_map.entrySet());

            ArrayList<Tuple2<Long,Links>> link_tuple_list = new ArrayList<Tuple2<Long,Links>>();
            Iterator<Map.Entry<Long,Links>> _iter = links_map_list.iterator();
            while (_iter.hasNext()) {
                Map.Entry<Long,Links> _value = _iter.next();
                link_tuple_list.add(new Tuple2<Long,Links>(_value.getKey(), _value.getValue()));
            }


            JavaPairRDD<Long,Links> tags_rdd =as.getSparkContext().parallelizePairs(link_tuple_list);

            return tags_rdd;
        }

      //Main function to test the logic and display the top 5 movies for new User 0
        public static void main(String args[]) throws IOException
        {
            //Initial Application and Spark Context.
           Application as=new Application();


            //train Model using small data
            int[] list ={8,6,4,10};  //choose rank between the below four options.

           //Assign Training Data file path to variables for loading files
           String _directory ="datafiles/train_dataset";
           final String _movies="movies.csv";
           String _ratings="ratings.csv";
           String _tags="tags.csv";
           String _links="links.csv";

           //Training Data constructor
           MovieDataLoader train_loader =new MovieDataLoader(_directory
                                                       ,_links
                                                       ,_tags
                                                       ,_movies
                                                       ,_ratings);


           //Create Movies Pair RDD with Movie ID and Movie class
            JavaPairRDD<Integer,Movie> train_movies_rdd=train_loader.loadMovieData(as);
           System.out.println("Movies :"+train_movies_rdd.count());

           /*System.out.println(LocalDateTime.now());

           JavaPairRDD<Long,UserTags> tags_rdd=_loader.loadUserTagData(as);
           System.out.println(tags_rdd.count());

           System.out.println(LocalDateTime.now());

           JavaPairRDD<Long,Links> link_rdd=_loader.loadIMDBLinkData(as);
           System.out.println(link_rdd.count());

           System.out.println(LocalDateTime.now());*/

            //Load the ratings data and generate Ratings RDD.
           JavaPairRDD<Long, Ratings> train_ratings_rdd=train_loader.loadRatingsData(as);
           System.out.println("Ratings: "+train_ratings_rdd.count());

           //Train the model using 100K dataset and get the best rank value that can be used for training model with large dataset.
          ModelTraining _train=new ModelTraining(train_ratings_rdd);
          String score[] =_train.returnBestRank(list).split(","); //get the best rank and RMSE received with training set.

          System.out.println("BEST RMSE on Train Data"+score[0]+"BEST Rank:"+score[1]);

         //Use the best rank with lowest error and build model using bigger data set.
            String large_directory ="datafiles";

            MovieDataLoader main_loader =new MovieDataLoader(large_directory
                    ,_links
                    ,_tags
                    ,_movies
                    ,_ratings);


            JavaPairRDD<Long, Ratings> ratings_rdd=main_loader.loadRatingsData(as);
            System.out.println("Ratings: "+train_ratings_rdd.count());

            //ModelBuilding _mat =new ModelBuilding(ratings_rdd);
          //_mat.setSpark_ratings_rdd(_mat.loadRatingRDD());

         //_mat.buildModel(Integer.parseInt(score[1]),20,0.01);


          //double _rmse =_mat.modelEvaluation();

         // System.out.println("Root Mean Square Error :"+_rmse);

            //_mat.recommendMovies();

          //  _mat.saveModel(as);

            //Create a new user not in dataset and assign few ratings by this user
            final int new_user_id=0;

            final List<Tuple3<Integer,Integer,Double>> new_user_ratings =new ArrayList<Tuple3<Integer, Integer, Double>>();
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id,260, (double) 4)); // Star Wars (1977)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 3, (double) 3)); // Toy Story (1995)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 16, (double) 3)); // Casino (1995)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 25, (double) 4)); // Leaving Las Vegas (1995)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 32,(double) 4)); // Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 335,(double) 1)); // Flintstones, The (1994)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 379,(double) 1)); // Timecop (1994)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 296,(double) 3)); // Pulp Fiction (1994)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 858,(double) 5)); // Godfather, The (1972)
            new_user_ratings.add(new Tuple3<Integer, Integer, Double>(new_user_id, 50,(double) 4));// Usual Suspects, The (1995)

            //RDD with new user ratings for 10 movies
            JavaRDD<Tuple3<Integer,Integer,Double>> new_user_ratings_rdd = as.getSparkContext().parallelize(new_user_ratings);

            //Create Spark ML lib Ratings RDD for the new user
            JavaRDD<Rating> new_rating_rdd =new_user_ratings_rdd.map(new Function<Tuple3<Integer, Integer, Double>, Rating>() {
                @Override
                public Rating call(Tuple3<Integer, Integer, Double> newRatings) throws Exception {
                    return new Rating(newRatings._1(),newRatings._2(),newRatings._3());
                }
            });

            //get the movie id's rated by the new user, this will be used to generate unrated movie rdd
            JavaRDD<Integer> rated_movies=new_rating_rdd.map(new Function<Rating, Integer>() {
                @Override
                public Integer call(Rating rating) throws Exception {
                    return rating.product();
                }
            });

            System.out.println("Rated Movie Count"+rated_movies.count());

            //Full rating rdd generated from the big ratings.csv file.
            JavaRDD<Rating> old_rating_rdd =ratings_rdd.map(new Function<Tuple2<Long, Ratings>, Rating>() {
                @Override
                public Rating call(Tuple2<Long, Ratings> ratingsTuple2) throws Exception {
                    return new Rating((int) ratingsTuple2._2().getUserID(),(int)ratingsTuple2._2().getMovieID(),ratingsTuple2._2().getRatings());
                }
            });

            //Form a combined ratings RDD by joining full ratings rdd from file and new user ratings.
            JavaRDD<Rating> combined_ratings_rdd=old_rating_rdd.union(new_rating_rdd);

            System.out.println("Combined RDD :"+combined_ratings_rdd.count());

            //Build the model using new ratings
            ModelBuilding _mat =new ModelBuilding(combined_ratings_rdd);
            //_mat.setSpark_ratings_rdd(_mat.loadRatingRDD());

            _mat.buildModel(Integer.parseInt(score[1]),20,0.01);

            //Evaluate the new model and get the Root Mean Sqaure Error
             double _rmse =_mat.modelEvaluation();

             System.out.println("Root Mean Square Error :"+_rmse);


            //Read all the movies from the large movie file -34000 movie data
            JavaPairRDD<Integer,Movie> movies_rdd=main_loader.loadMovieData(as);
            System.out.println("All Movies :"+movies_rdd.count());

            //Get the movie IDs of 34000 movies
            JavaRDD<Integer> all_movie_rdd=movies_rdd.map(new Function<Tuple2<Integer, Movie>, Integer>() {
                @Override
                public Integer call(Tuple2<Integer, Movie> intMovieTuple2) throws Exception {
                    return intMovieTuple2._2().getMovieID();
                }
            });

            System.out.println("All Movies IDs:"+all_movie_rdd.count());


            //Get the unrated movies by the new user
            JavaRDD<Integer> unrated_movies=all_movie_rdd.subtract(rated_movies);

            System.out.println("Unrated Movies :"+unrated_movies.count());

            //Generate RDD with new user id 0 and all the unrated movies in the movie
            JavaRDD<Tuple2<Object,Object>> new_userMovies =unrated_movies.map(new Function<Integer, Tuple2<Object, Object>>() {
                @Override
                public Tuple2<Object, Object> call(Integer movie) throws Exception {
                    return new Tuple2<Object, Object>(new_user_id,movie);
                }
            });

            //Call the remomendation method to get the predicted movies with ratings greater than 3.5
            JavaPairRDD<Integer,Double> recommendation=_mat.recommendMovies(new_userMovies);


            JavaPairRDD<Integer,String> movie_titles_rdd=movies_rdd.mapToPair(new PairFunction<Tuple2<Integer, Movie>, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Tuple2<Integer, Movie> movietitles) throws Exception {
                    return new Tuple2<Integer, String>(movietitles._1(),movietitles._2.getMovieName());
                }
            });

            //Join the recommendation rdd with movie titles and pick the top 5 movies for the user.
             JavaPairRDD<Integer,Tuple2<Double,String>> recommended_movie_titles_rdd = recommendation.join(movie_titles_rdd);

            List<Tuple2<Integer,Tuple2<Double,String>>> movie_recommend_new_user=recommended_movie_titles_rdd.take(10);

            System.out.println("Predicted Movie Title,Predicted Ratings");
            for(int recommend_movie_i=0;recommend_movie_i<movie_recommend_new_user.size();recommend_movie_i++) {
                System.out.println(movie_recommend_new_user.get(recommend_movie_i)._2._2+","+movie_recommend_new_user.get(recommend_movie_i)._2._1);
            }
        }

  }









            //JavaPairRDD<Object,Object> unrated_movie_rdd_final =


            //System.out.println("Unrated Movies :"+unrated_movie_rdd_final.count());

            //String results[]=_mat.recommendMovies(unrated_movie_rdd);




            //JavaRDD<Rating> filtered_rdd=  old_rating_rdd.subtract(new_rating_rdd);

//            JavaRDD<Tuple2<Object,Rating[]>> recommended_movies=_mat.recommendMovies(0);
//
  //          System.out.println("Back from Recommend Movies"+recommended_movies.count());

            /*recommended_movies.foreach(new VoidFunction<Tuple2<Object, Rating[]>>() {
                @Override
                public void call(Tuple2<Object, Rating[]> objectTuple2) throws Exception {
                    System.out.println("User"+objectTuple2._1);

                    for(int movies_i=0;movies_i<objectTuple2._2().length;movies_i++)
                    {
                        System.out.println(objectTuple2._2.toString());
                    }


                }
            });*/





// JavaRDD<Rating> new_user_rating =old_rating_rdd.subtract(new_rating_rdd);

//System.out.println("New User RDD :"+new_user_rating.count());
