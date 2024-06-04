package model;

import com.google.gson.Gson;

/***
 * Generated Lift Ride Event
 */
public class LiftRideEvent {
    private int skierID;
    private int resortID;
    private int liftID;
    private String seasonID;
    private String dayID;
    private int time;

    // Constructor
    public LiftRideEvent(int skierID, int resortID, int liftID, String seasonID, String dayID, int time) {
        this.skierID = skierID;
        this.resortID = resortID;
        this.liftID = liftID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.time = time;
    }

    public LiftRideEvent() {

    }

    public int getSkierID() {
        return skierID;
    }

    public void setSkierID(int skierID) {
        this.skierID = skierID;
    }

    public int getResortID() {
        return resortID;
    }

    public void setResortID(int resortID) {
        this.resortID = resortID;
    }

    public int getLiftID() {
        return liftID;
    }

    public void setLiftID(int liftID) {
        this.liftID = liftID;
    }

    public String getSeasonID() {
        return seasonID;
    }

    public void setSeasonID(String seasonID) {
        this.seasonID = seasonID;
    }

    public String getDayID() {
        return dayID;
    }

    public void setDayID(String dayID) {
        this.dayID = dayID;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
