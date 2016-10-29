package data.loader;

import java.io.*;
import java.util.*;
/**
 * Created by Rash on 29-10-2016.
 */
public class Movie {

    private long movieID;
    private String movieName;
    private Map<String,Integer> genreList;

    enum genres {

    }

    public Movie(long movieID, Map<String,Integer> genreList, String movieName) {
        this.movieID = movieID;
        this.genreList = genreList;
        this.movieName = movieName;
    }
}


