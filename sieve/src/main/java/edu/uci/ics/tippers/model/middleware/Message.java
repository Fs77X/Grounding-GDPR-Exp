package edu.uci.ics.tippers.model.middleware;

import edu.uci.ics.tippers.dbms.MallData;

public class Message{
    private String msg;
    private MallData[] md;
    private MetaData[] metaData;
    private String uid;
    private String qid;
    public Message(String msg, MallData[] md, MetaData[] metaData, String qid, String uid) {
        this.msg = msg;
        this.md = md;
        this.metaData = metaData; 
        this.qid = qid;
        this.uid = uid;
    }
    
    public void setMessage(String msg) {
        this.msg = msg;
    }

    public void setMD(MallData[] mall) {
        this.md = mall;
    }

    public String getMessage() {
        return msg;
    }

    public MallData[] getMD() {
        return this.md;
    }

    public MetaData[] getMetaData() {
        return this.metaData;
    }

    public String getUID() {
        return this.uid;
    }

    public String getQid() {
        return this.qid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Message{");
        sb.append("msg = ").append(msg).append("\n");
        sb.append(", md = [\n");
        for (int i = 0; i < this.md.length; i++) {
            sb.append("{").append("id: ").append(md[i].getId()).append(", ");
            sb.append("shop_name: ").append(md[i].getShopName()).append(", ");
            sb.append("obs_date: ").append(md[i].getObsDate()).append(", ");
            sb.append("obs_time: ").append(md[i].getObsTime()).append(", ");
            sb.append("user_interest: ").append(md[i].getUserInterest()).append(", ");
            sb.append("device_id: ").append(md[i].getDeviceID()).append("}");
            if (i != this.md.length - 1) {
                sb.append(",\n");
            }
        }
        sb.append("\n]");
        return sb.toString();
    }
 
    
}
