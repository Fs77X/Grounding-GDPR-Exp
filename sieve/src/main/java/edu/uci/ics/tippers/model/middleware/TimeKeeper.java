package edu.uci.ics.tippers.model.middleware;

public class TimeKeeper {
    private long departTime;
    private long arriveTime;
    public TimeKeeper(long departTime, long arriveTime) {
        this.departTime = departTime;
        this.arriveTime = arriveTime;
    }

    public long getDepartTime() {
        return this.departTime;
    }

    public long getArriveTime() {
        return this.arriveTime;
    }
    
}
