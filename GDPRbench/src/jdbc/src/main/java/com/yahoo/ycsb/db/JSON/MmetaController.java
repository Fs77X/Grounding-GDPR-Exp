package com.yahoo.ycsb.db.JSON;

public class MmetaController {
    private String prop;
    private String info;
    private String id;
    private String changeVal;
    public MmetaController(String prop, String info, String id, String changeVal) {
        this.prop = prop;
        this.info = info;
        this.id = id;
        this.changeVal = changeVal;
    }
    public String getProp() {
        return this.prop;
    }

    public String getInfo() {
        return this.info;
    }

    public String getID() {
        return this.id;
    }

    public String getChangeVal() {
        return this.changeVal;
    }
    
}
