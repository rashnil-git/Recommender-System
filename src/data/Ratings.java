package data;

import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Rash on 30-10-2016.
 */
public class Ratings implements Serializable {

    private long movieID;
    private ArrayList<Tuple3<Long,Double,Long>> ratings;

    public Ratings(long movieID,ArrayList<Tuple3<Long, Double, Long>> ratings) {
        this.ratings = ratings;
        this.movieID = movieID;
    }

    public ArrayList<Tuple3<Long, Double, Long>> getRatings() {
        return ratings;
    }
}
