package data;

import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Rash on 30-10-2016.
 */
public class Ratings implements Serializable {

    private long movieID;
    private long userID;
    private double ratings;
    private long timestamp;
    //ArrayList<Tuple3<Long,Double,Long>> ratings;


    public Ratings(long movieID, long userID, double ratings, long timestamp) {
        this.movieID = movieID;
        this.userID = userID;
        this.ratings = ratings;
        this.timestamp = timestamp;
    }

    public long getMovieID() {
        return movieID;
    }

    public long getUserID() {
        return userID;
    }

    public double getRatings() {
        return ratings;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
