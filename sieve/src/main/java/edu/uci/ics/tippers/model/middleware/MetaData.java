package edu.uci.ics.tippers.model.middleware;
/*
CREATE TABLE public.user_policy (
    policy_id integer NOT NULL,
    id character varying(255) NOT NULL,
    querier character varying(255) NOT NULL,
    purpose character varying(255) NOT NULL,
    ttl integer NOT NULL,
    origin character varying(255) NOT NULL,
    objection character varying(255) NOT NULL,
    sharing character varying(255) NOT NULL,
    enforcement_action character varying(255),
    inserted_at timestamp with time zone NOT NULL,
    device_id integer,
    key character varying(50)
);




*/

public class MetaData {
    private int policy_id;
    private String id;
    private String querier;
    private String purpose;
    private int ttl;
    private String origin;
    private String objection;
    private String sharing;
    private String enforcement_action;
    private String inserted_at;
    private int device_id;
    private String key;

    public MetaData() {

    }

    public MetaData(int policy_id, String id, String querier, String purpose, int ttl, String origin, String objection, String sharing, String enforcement_action, String inserted_at, int device_id, String key) {
        this.policy_id = policy_id;
        this.id = id;
        this.querier = querier;
        this.purpose = purpose;
        this.ttl = ttl;
        this.origin = origin;
        this.objection = objection;
        this.sharing = sharing;
        this.enforcement_action = enforcement_action;
        this.inserted_at = inserted_at;
        this.device_id = device_id;
        this.key = key;

    }

    public int getPolicyID() {
        return this.policy_id;
    }

    public String getID() {
        return this.id;
    }

    public String getQuerier() {
        return this.querier;
    }

    public String getPurpose() {
        return this.purpose;
    }

    public int getTtl() {
        return this.ttl;
    }

    public String getOrigin() {
        return this.origin;
    }

    public String getObjection() {
        return this.objection;
    }

    public String getSharing() {
        return this.sharing;
    }

    public String getEnforcementAction() {
        return this.enforcement_action;
    }

    public String getInsertedAt() {
        return this.inserted_at;
    }

    public int getDeviceID() {
        return this.device_id;
    }

    public String getKey() {
        return this.key;
    }

    
}
