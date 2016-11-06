package data.loaders;

/**
 * Created by Rash on 29-10-2016.
 */

import data.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.joda.time.LocalDateTime;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterable;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


  public class MovieDataLoader implements Serializable{

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
        public JavaPairRDD<Long, Movie> loadMovieData(Application as) throws IOException
        {
            long movieID;
            String movieName;
            Map<Long,Movie> movie_map =new HashMap<Long, Movie>();

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

            ArrayList<Tuple2<Long,Movie>> movie_tuple_list = new ArrayList<Tuple2<Long,Movie>>();
            Iterator<Map.Entry<Long,Movie>> _iter = movie_map_list.iterator();
            while (_iter.hasNext()) {
                Map.Entry<Long,Movie> _value = _iter.next();
                movie_tuple_list.add(new Tuple2<Long,Movie>(_value.getKey(), _value.getValue()));
            }


            JavaPairRDD<Long,Movie> movies_rdd =as.getSparkContext().parallelizePairs(movie_tuple_list);

            return movies_rdd;

        }

        public JavaPairRDD<Long,Ratings> loadRatingsData(Application as)
        {

            String ratings_path=this.dataDirectory+File.separator+this.ratingData;

            JavaRDD<String> ratings_file_rdd= as.getSparkContext().textFile(ratings_path);

            JavaRDD<String> ratings_data_rdd= ratings_file_rdd.mapPartitionsWithIndex(_header,false);


            JavaPairRDD<Long,Ratings> ratings_rdd  = ratings_data_rdd.mapToPair(new PairFunction<String, Long, Ratings>() {
                @Override
                public Tuple2<Long, Ratings> call(String s) throws Exception {
                    long movieID;

                    String rat_list_string[] = s.split(",");
                    movieID=Long.valueOf(rat_list_string[1]);
                    long userID= Long.valueOf(rat_list_string[0]);
                    double ratings=Double.valueOf(rat_list_string[2]);
                    long timestamp =Long.valueOf(rat_list_string[3]);

                   // ArrayList ratings_list =new ArrayList<Tuple3<Long, Double, Long>>();
                    //ratings_list.add(new Tuple3<Long, Double, Long>(userID,ratings,timestamp));


                    Ratings _ratings=new Ratings(movieID,userID,ratings,timestamp);

                    return (new Tuple2<Long,Ratings>(movieID,_ratings));
                }
            });

        //    JavaPairRDD<Long, java.lang.Iterable<Ratings>> final_ratings_rdd =ratings_rdd.groupByKey();

            return ratings_rdd;

            }


        public JavaPairRDD<Long,UserTags> loadUserTagData(Application as) throws IOException
            {
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



        public JavaPairRDD<Long,Links> loadIMDBLinkData(Application as)
        {
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

        public static void main(String args[]) throws IOException
        {
           Application as=new Application();

           String _directory ="datafiles";
           String _movies="movies.csv";
           String _ratings="ratings.csv";
           String _tags="tags.csv";
           String _links="links.csv";

           MovieDataLoader _loader =new MovieDataLoader(_directory
                                                       ,_links
                                                       ,_tags
                                                       ,_movies
                                                       ,_ratings);

           System.out.println(LocalDateTime.now());

           JavaPairRDD<Long,Movie> movies_rdd=_loader.loadMovieData(as);
           System.out.println(movies_rdd.count());

           System.out.println(LocalDateTime.now());

           JavaPairRDD<Long,UserTags> tags_rdd=_loader.loadUserTagData(as);
           System.out.println(tags_rdd.count());

           System.out.println(LocalDateTime.now());

           JavaPairRDD<Long,Links> link_rdd=_loader.loadIMDBLinkData(as);
           System.out.println(link_rdd.count());

           System.out.println(LocalDateTime.now());

           JavaPairRDD<Long, Ratings> ratings_rdd=_loader.loadRatingsData(as);
           System.out.println(ratings_rdd.count());

           System.out.println(LocalDateTime.now());


          MatrixBuilding _mat =new MatrixBuilding(ratings_rdd);
          //_mat.setSpark_ratings_rdd(_mat.loadRatingRDD());

          _mat.buildModel(10,10,0.01);

          double _mse =_mat.modelEvaluation();

          System.out.println("Root Mean Square Error :"+_mse);

            _mat.recommendMovies();


        }

    }



