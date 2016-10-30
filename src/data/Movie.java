package data;

import java.io.*;
import java.util.*;
/**
 * Created by Rash on 29-10-2016.
 */
public class Movie implements Serializable{

    private long movieID;
    private String movieName;
    private List<String> genreList;

    public Movie(long movieID, String movieName,List<String> genreList) {
        this.movieID = movieID;
        this.genreList = genreList;
        this.movieName = movieName;
    }

    public long getMovieID() {
        return movieID;
    }

    public String getMovieName() {
        return movieName;
    }

    public List<String> getGenreList() {
        return genreList;
    }
}


