package edu.uci.ics.tippers.execution.MiddleWare;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.MallData;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.dbms.QueryResult;
// import edu.uci.ics.tippers.execution.ExpResult;
// import edu.uci.ics.tippers.fileop.Writer;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.producer.LogSieve;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.guard.SelectGuard;
import edu.uci.ics.tippers.model.middleware.MetaData;
import edu.uci.ics.tippers.model.middleware.PuciLog;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
// import java.time.*;
import java.util.*;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
// import java.sql.Date;
import java.util.Date;
import edu.uci.ics.tippers.fileop.Writer;

public class ops {
    private static QueryManager queryManager;
    private int removeUserData(String device_id) {
        PolicyConstants.initialize();
        String query = "DELETE from mall_observation WHERE device_id = " + device_id;
        return queryManager.runMidDelMod(query);
    }

    public String[] getPolicyIdFromEntry(String key) {
        String query = "SELECT id from user_policy WHERE key = \'" + key + "\';";
        queryManager = new QueryManager();
        return queryManager.getPolId(query);
    }

    public int removeUserEntry(String key) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "DELETE from mall_observation WHERE id = \'" + key + "\';";
        int status = queryManager.runMidDelMod(query);
        query = "VACUUM FULL mall_observation";
        // get polid first, then delete
        String[] polid = getPolicyIdFromEntry(key);
        query = "DELETE from user_policy WHERE key = \'" + key + "\';";
        status = queryManager.runMidDelMod(query);
        query = "VACUUM FULL user_policy";
        queryManager.runMidDelMod(query);
        query = "DELETE from user_policy_object_condition WHERE policy_id = \'" + polid[0] + "\';";
        status = queryManager.runMidDelMod(query);
        query = "VACUUM FULL user_policy_object_condition";
        queryManager.runMidDelMod(query);
        PuciLog pl = new PuciLog(key, "DELETE", "succ", "true");
        try {
            LogSieve ls = new LogSieve();
            ls.sendResults(pl);
        } catch (Exception e) { e.printStackTrace(); }
        return status;
    }

    private int removeUserOC(String[] policy_id) {
        PolicyConstants.initialize();
        StringBuilder q = new StringBuilder("DELETE from user_policy_object_condition WHERE ");
        for (int i = 0; i < policy_id.length; i++) {
            q.append("policy_id = ").append(policy_id[i]);
            if (i == policy_id.length-1) q.append(" AND ");
        }
        return queryManager.runMidDelMod(q.toString());
    }
    private int removeUserPolicy(String[] policy_id) {
        PolicyConstants.initialize();
        StringBuilder q = new StringBuilder("DELETE from user_policy WHERE ");
        for (int i = 0; i < policy_id.length; i++) {
            q.append("id = ").append(policy_id[i]);
            if (i == policy_id.length-1) q.append(" AND ");
        }
        return queryManager.runMidDelMod(q.toString());
    }
    private String[] getPolicyIdFromUsr(String device_id) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "SELECT UNIQUE policy_id from user_policy_object_condition WHERE attribute = device_id AND comp_value = " + device_id;
        return queryManager.getPolId(query);
    }

    private int insertEntry(MallData mallData) {
        int device_id = mallData.getDeviceID();
        String key = mallData.getId();
        String shop_name = mallData.getShopName();
        Date obs_date = mallData.getObsDate();
        Time obs_time = mallData.getObsTime();
        String user_interest = mallData.getUserInterest();
        
        
        StringBuilder sb = new StringBuilder("INSERT INTO mall_observation(id, shop_name, obs_date, obs_time, user_interest, device_id) VALUES(");
        sb.append("\'").append(key).append("\', ");
        sb.append("\'").append(shop_name).append("\', ");
        sb.append("\'").append(obs_date).append("\', ");
        sb.append("\'").append(obs_time).append("\', ");
        sb.append("\'").append(user_interest).append("\', ");
        sb.append("\'").append(device_id).append("\');");
        return queryManager.runMidDelMod(sb.toString());
    }

    private int insertPolicyEntry(MetaData metaData) {
        String dateString = metaData.getInsertedAt();
        Timestamp timestamp = null;
        try {
            Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(dateString);
            String formattedDate = new SimpleDateFormat("yyyyMMdd").format(date);
            timestamp = new Timestamp(new SimpleDateFormat("yyyyMMdd").parse(formattedDate).getTime());
            System.out.println("TIMESTAMP: " + timestamp);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder("INSERT INTO user_policy(policy_id, id, querier, purpose, ttl, origin, objection, sharing, enforcement_action, inserted_at, device_id, key) VALUES(");
        sb.append("").append(metaData.getPolicyID()).append(", ");
        sb.append("\'").append(metaData.getID()).append("\', ");
        sb.append("\'").append(metaData.getQuerier()).append("\', ");
        sb.append("\'").append(metaData.getPurpose()).append("\', ");
        sb.append("").append(metaData.getTtl()).append(", ");
        sb.append("\'").append(metaData.getOrigin()).append("\', ");
        sb.append("\'").append(metaData.getObjection()).append("\', ");
        sb.append("\'").append(metaData.getSharing()).append("\', ");
        sb.append("\'").append(metaData.getEnforcementAction()).append("\', ");
        sb.append("\'").append(timestamp).append("\', ");
        sb.append("").append(metaData.getDeviceID()).append(", ");
        sb.append("\'").append(metaData.getKey()).append("\');");
        // System.out.println(sb.toString());
        String query = sb.toString();
        PuciLog pl = new PuciLog(metaData.getKey(), query, "succ", "false");
        try {
            LogSieve ls = new LogSieve();
            ls.sendResults(pl);
        } catch (Exception e) { e.printStackTrace(); }
        return queryManager.runMidDelMod(query);

    }

    public String buildOCInsert(MallData mallData, String polID, String col, String operator, int counter) {
        StringBuilder sb = new StringBuilder("INSERT INTO user_policy_object_condition(id, policy_id, attribute, attribute_type, operator, comp_value) VALUES(");
        sb.append(counter).append(", ");
        sb.append("\'").append(polID).append("\', ");
        sb.append("\'").append(col).append("\', ");
        switch (col) {
            case "obs_date":
            sb.append("\'").append("DATE").append("\', ");
                break;
            case "obs_time":
            sb.append("\'").append("TIME").append("\', ");
                break;
            default:
                sb.append("\'").append("STRING").append("\', ");
        }
        sb.append("\'").append(operator).append("\', ");
        switch (col) {
            case "device_id":
                sb.append("\'").append(mallData.getDeviceID()).append("\')");
                break;
            case "shop_name":
                sb.append("\'").append(mallData.getShopName()).append("\')");
                break;
            case "obs_time":
                sb.append("\'").append(mallData.getObsTime()).append("\')");
                break;
            case "obs_date":
                sb.append("\'").append(mallData.getObsDate()).append("\')");
                break;
            case "user_interest":
                sb.append("\'").append(mallData.getUserInterest()).append("\')");
                break;
            default:
                System.err.println("RUH ROH");
        }
        return sb.toString();
    }

    public int InsertOC(MallData mallData, String polID, int counter) {
        final String[] mdCols = {"device_id", "shop_name", "obs_date", "obs_time", "user_interest"};
        int status = 0;
        for(int i = 0; i < mdCols.length; i++) {
            if(mdCols[i].equals("user_interest") && !mallData.getUserInterest().isEmpty()) {
                String query = buildOCInsert(mallData, polID, mdCols[i], "=", counter);
                PuciLog pl = new PuciLog(mallData.getId(), query, "succ", "false");
                try {
                    LogSieve ls = new LogSieve();
                    ls.sendResults(pl);
                } catch (Exception e) { e.printStackTrace(); }
                counter = counter + 1;
                status = queryManager.runMidDelMod(query);
                query = buildOCInsert(mallData, polID, mdCols[i], "=", counter);
                pl = new PuciLog(mallData.getId(), query, "succ", "false");
                try {
                    LogSieve ls = new LogSieve();
                    ls.sendResults(pl);
                } catch (Exception e) { e.printStackTrace(); }
                counter = counter + 1;
                status = queryManager.runMidDelMod(query);
            } else {
                if(mdCols[i].equals("obs_date") || mdCols[i].equals("obs_time")) {
                    String query = buildOCInsert(mallData, polID, mdCols[i], "<=", counter);
                    PuciLog pl = new PuciLog(mallData.getId(), query, "succ", "false");
                    try {
                        LogSieve ls = new LogSieve();
                        ls.sendResults(pl);
                    } catch (Exception e) { e.printStackTrace(); }
                    counter = counter + 1;
                    status = queryManager.runMidDelMod(query);
                    query = buildOCInsert(mallData, polID, mdCols[i], ">=", counter);
                    pl = new PuciLog(mallData.getId(), query, "succ", "false");
                    try {
                        LogSieve ls = new LogSieve();
                        ls.sendResults(pl);
                    } catch (Exception e) { e.printStackTrace(); }
                    counter = counter + 1;
                    status = queryManager.runMidDelMod(query);
                } else if(!mdCols[i].equals("user_interest")) {
                    // if (mdCols[i].equals("user_interest")) {
                    //     System.out.println("FELL HERE CUZ JAVA SUX2");
                    // }
                    String query = buildOCInsert(mallData, polID, mdCols[i], "=", counter);
                    PuciLog pl = new PuciLog(mallData.getId(), query, "succ", "false");
                    try {
                        LogSieve ls = new LogSieve();
                        ls.sendResults(pl);
                    } catch (Exception e) { e.printStackTrace(); }
                    counter = counter + 1;
                    status = queryManager.runMidDelMod(query);
                    query = buildOCInsert(mallData, polID, mdCols[i], "=", counter);
                    pl = new PuciLog(mallData.getId(), query, "succ", "false");
                    try {
                        LogSieve ls = new LogSieve();
                        ls.sendResults(pl);
                    } catch (Exception e) { e.printStackTrace(); }
                    counter = counter + 1;
                    status = queryManager.runMidDelMod(query);
                }
            }
        }
        if (status == 0) {
            return counter;
        } else {
            return -1;
        }
    }

    public int insertData(MallData mallData, MetaData metaData, int counter){
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String policy_id = metaData.getID();
        // String key = mallData.getId();
        int status = insertEntry(mallData);
        // status = insertPolicyEntry(metaData);
        // status = InsertOC(mallData, policy_id, counter);
        return status;
        
    }

    public int deletePersonalData(String device_id) {
        PolicyConstants.initialize();
        String[] polId = getPolicyIdFromUsr(device_id);
        int status = removeUserPolicy(polId);
        status = removeUserOC(polId);
        status = removeUserData(device_id);
        return status;
    }
    public MetaData[] getMetaDataKey(String device_id, String key) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "SELECT * from user_policy where device_id = \'" + device_id + "\' AND key = \'" + key + "\'";
        return queryManager.getMData(query, key);


    }
    public MallData[] getpersonalData(String device_id) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "SELECT * from mall_observation where device_id = " + device_id;
        return queryManager.runMiddleWareQuery(query);
    } 
    public MallData[] getpersonalEntry(String id) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "SELECT * from mall_observation where id = \'" + id +"\'";
        return queryManager.runMiddleWareQuery(query);
    } 
    public MallData[] get(String querier, String prop, String info) {
        /*
        * TODO: Get query results into a class array format and return it
        */
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        PolicyPersistor polper = PolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = polper.retrievePoliciesMid(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW, prop, info);
        if (bePolicies == null) {
            return null;
        }
        BEExpression beExpression = new BEExpression(bePolicies);
        SelectGuard gh = new SelectGuard(beExpression, true);
        System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
        GuardExp guardExp = gh.create();
        String guard_query_union = guardExp.queryRewrite(true, true);
        // System.out.println(guard_query_union);
        MallData[] mall = queryManager.runMiddleWareQuery(guard_query_union);
        // System.out.println("Took: " + execResultUnion.toString() + " ms");
        return mall;

    }
    public Integer delete(String querier, String prop, String info) {
        /*
        * TODO: Do what I did in get but in delete
        */
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        PolicyPersistor polper = PolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = polper.retrievePoliciesMid(querier, PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW, prop, info);
        if (bePolicies == null) {
            return null;
        }
        BEExpression beExpression = new BEExpression(bePolicies);
        SelectGuard gh = new SelectGuard(beExpression, true);
        System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
        GuardExp guardExp = gh.create();
        String guard_query_union = guardExp.queryRewriteMiddleWare(false, true, "DELETE");
        System.out.println(guard_query_union);
        Integer status = queryManager.runMidDelMod(guard_query_union);
        // System.out.print("Took: " + execResultUnion.getTimeTaken().toMillis() + " ms");
        return status;
    }

    public int updateEntry(String updateKey, String prop, String info) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "UPDATE mall_observation SET " + prop + "= \'" + info + "\' WHERE id = \'" + updateKey + "\';";
        return queryManager.runMidDelMod(query);
    }

    public int updateMeta(String changeVal, String prop, String info, String querier) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "";
        query = "UPDATE user_policy SET " + prop + "= \'" + changeVal + "\' WHERE " + prop + "= \'" + info + "\' AND querier = \'" + querier + "\'";
        System.out.println(query);
        int stat = queryManager.runMidDelMod(query);
        PuciLog pl = new PuciLog(querier, query, "succ", "false");
        try {
            LogSieve ls = new LogSieve();
            ls.sendResults(pl);
        } catch (Exception e) { e.printStackTrace(); }
        return stat;
    }

    public int updateMetaEntry(String updateKey, String prop, String info) {
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        String query = "UPDATE user_policy SET " + prop + "= \'" + info + "\' WHERE key = \'" + updateKey + "\';";
        System.out.println(query);
        int stat = queryManager.runMidDelMod(query);
        PuciLog pl = new PuciLog(updateKey, query, "succ", "false");
        try {
            LogSieve ls = new LogSieve();
            ls.sendResults(pl);
        } catch (Exception e) { e.printStackTrace(); }
        return stat;
    }

    public void update(){
        PolicyConstants.initialize();
        queryManager = new QueryManager();
        System.out.println("Running on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
                +  PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
        PolicyPersistor polper = PolicyPersistor.getInstance();
        List<BEPolicy> bePolicies = polper.retrievePolicies("7", PolicyConstants.USER_INDIVIDUAL, PolicyConstants.ACTION_ALLOW);
        
        BEExpression beExpression = new BEExpression(bePolicies);
        SelectGuard gh = new SelectGuard(beExpression, true);
        System.out.println("Number of policies: " + beExpression.getPolicies().size() + " Number of Guards: " + gh.numberOfGuards());
        GuardExp guardExp = gh.create();
        String guard_query_union = guardExp.queryRewriteMiddleWare(true, true, "UPDATE");
        System.out.println(guard_query_union);
        QueryResult execResultUnion = queryManager.runTimedQueryExp(guard_query_union, 1);
        System.out.print("Took: " + execResultUnion.getTimeTaken().toMillis() + " ms");
        return;
    }
    
}
