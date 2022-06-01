package com.yahoo.ycsb.db.JSON;
/**
 * mgetentry
 */
public class MgetEntry {
    private String id;
    private String device_id;
    public MgetEntry(String id, String device_id){
        this.id = id;
        this.device_id = device_id;
    }

    public String getId() {
        return this.id;
    }

    public String getDeviceId() {
        return this.device_id;
    }
    
}