package edu.uci.ics.tippers.execution.MiddleWare;

public class PropInfo {
    private String prop;
    private String info;

    public PropInfo(String prop, String info) {
        this.prop = prop;
        this.info = info;
    }

    public PropInfo(PropInfo propInfo) {
        this.prop = propInfo.getProp();
        this.info = propInfo.getInfo();
    }
    public String getProp() {
        return prop;
    }

    public void setProp(String prop) {
        this.prop = prop;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }
    
}