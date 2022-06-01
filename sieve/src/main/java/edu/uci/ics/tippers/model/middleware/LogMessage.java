package edu.uci.ics.tippers.model.middleware;
public class LogMessage {
    private String querier;
    private String log;
    public LogMessage(String querier, String operation) {
        this.querier = querier;
        this.log = operation;
    }
    public String getQuerier() {
        return this.querier;
    }
    public String getLog() {
        return this.log;
    }
    
}
