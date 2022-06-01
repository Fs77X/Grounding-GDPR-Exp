package edu.uci.ics.tippers.model.middleware;

public class PuciLog {
    private String querier;
    private String operation;
    private String result;
    private String delete;

    public PuciLog(String querier, String operation, String result, String delete) {
        this.querier = querier;
        this.operation = operation;
        this.result = result;
        this.delete = delete;
    }

    public void setQuerier(String querier) {
        this.querier = querier;
    }

    public String getQuerier() {
        return this.querier;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getOperation() {
        return this.operation;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getResult() {
        return this.result;
    }

    public void setDelete(String delete) {
        this.delete = delete;
    }

    public String getDelete() {
        return this.delete;
    }
    
}
