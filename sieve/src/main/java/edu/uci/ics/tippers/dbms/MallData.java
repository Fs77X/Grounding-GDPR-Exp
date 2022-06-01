package edu.uci.ics.tippers.dbms;

import java.sql.Time;
import java.sql.Date;
public class MallData {
    private String id;
    private String shop_name;
    private Date obs_date;
    private Time obs_time;
    private String user_interest;
    private Integer device_id;

    public MallData() {

    }
    public MallData(String id, String shop_name, Date obs_date, Time obs_time, String user_interest, Integer device_id) {
        this.id = id;
        this.shop_name = shop_name;
        this.obs_date = obs_date;
        this.obs_time = obs_time;
        this.user_interest = user_interest;
        this.device_id = device_id;
    }

    public String getId() {
        return this.id;
    }

    public String getShopName() {
        return this.shop_name;
    }

    public Date getObsDate() {
        return this.obs_date;
    }

    public Time getObsTime() {
        return this.obs_time;
    }

    public String getUserInterest() {
        return this.user_interest;
    }

    public Integer getDeviceID() {
        return this.device_id;
    }
}
