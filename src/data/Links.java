package data;

import java.io.Serializable;

/**
 * Created by Rash on 30-10-2016.
 */
public class Links implements Serializable {

    private long movieID;
    private long imdbId;
    //private long tmdbId;

    public Links(long movieID, long imdbId) {
        this.movieID = movieID;
        this.imdbId = imdbId;
        //this.tmdbId = tmdbId;
    }
}
