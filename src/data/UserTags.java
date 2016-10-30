package data;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Rash on 30-10-2016.
 */
public class UserTags implements Serializable {

    private long UserID;
    private long movieID;
    private ArrayList<String> tag;
    private double timestamp;

    public UserTags(long userID, long movieID, ArrayList<String> tag, double timestamp) {
        UserID = userID;
        this.movieID = movieID;
        this.tag = tag;
        this.timestamp = timestamp;
    }

    public long getUserID() {
        return UserID;
    }

    public long getMovieID() {
        return movieID;
    }

    public ArrayList<String> getTag() {
        return tag;
    }

    public double getTimestamp() {
        return timestamp;
    }
}
