package edu.uci.ics.tippers.execution.MiddleWare;

import edu.uci.ics.tippers.dbms.MallData;

public class Message{
    private String msg;
    private MallData[] md;
    public Message(String msg, MallData[] md) {
        this.msg = msg;
        this.md = md; 
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
 
    
}
