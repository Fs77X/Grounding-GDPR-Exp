package edu.uci.ics.tippers.execution.MiddleWare;
public class mget_obj {
    private String[] id;
    private PropInfo[] property;
    public mget_obj(String[] id, PropInfo[] property) {
        this.id = id;
        this.property = property;
    }
    public String[] getId() {
        return id;
    }

    public PropInfo[] getProp() {
        return property;
    }
}


