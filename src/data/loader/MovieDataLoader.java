package data.loader;

/**
 * Created by Rash on 29-10-2016.
 */

import org.apache.commons.configuration2.INIConfiguration;
import org.apache.commons.configuration2.SubnodeConfiguration;
import org.apache.oro.text.regex.Pattern;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


  public class MovieDataLoader {

      private String dataDirectory;
      private String movieData;
      private String ratingData;
      private String userTagsData;
      private String movieLinkData;


      //Constructor
      public MovieDataLoader(String dataDirectory, String movieLinkData, String userTagsData, String movieData, String ratingData) {
          this.dataDirectory = dataDirectory;
          this.movieLinkData = movieLinkData;
          this.userTagsData = userTagsData;
          this.movieData = movieData;
          this.ratingData = ratingData;
      }

      //load the movie data
        public JavaRDD<Movie> loadMovieData(Application as)
        {
            long movieID;
            String movieName;
            Map<String,Integer> movieGenreList;

            String movie_path=this.dataDirectory+File.separator+this.movieData;

            JavaRDD<String> movie_file_rdd= as.getSparkContext().textFile(movie_path);

            List<String> movie_list =movie_file_rdd.collect();

            for(int movie_list_i=0;movie_list_i<movie_list.size();movie_list_i++)
            {
                String movie_list_string []=movie_list.get(movie_list_i).split(",");

                movieID=Integer.parseInt(movie_list_string[0]);
                movieName =movie_list_string[1];

                String genres[] =movie_list_string[2].split("\\|");

                for (int genre_i=0;genre_i<genres.length;genre_i++)
                {



                }




            }

            JavaRDD<Movie> movies_rdd =null;

            return movies_rdd;

        }

        public void loadRatingsData()
        {

        }

        public void loadUserTagData()
        {

        }

        public void loadIMDBLinkData()
        {

        }

        public static void main(String args[])
        {





        }

    }



