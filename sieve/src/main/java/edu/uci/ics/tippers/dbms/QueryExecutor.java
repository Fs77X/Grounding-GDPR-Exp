package edu.uci.ics.tippers.dbms;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.model.middleware.MetaData;
import edu.uci.ics.tippers.model.middleware.PuciLog;
import edu.uci.ics.tippers.producer.LogSieve;

import org.apache.commons.dbutils.DbUtils;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.*;

public class QueryExecutor {

    private long timeout = 0;

    private final Connection connection;

    public QueryExecutor(Connection connection, long timeout){
        this.connection = connection;
        this.timeout = timeout + PolicyConstants.MAX_DURATION.toMillis();
    }

    /**
     * 
     * @param query String of query
     * @return 0 or throws SQLException
     * Performs insert, delete, modify queries
     */
    public Integer runDelModQuery(String query) {
        Statement statement = null;
        try{
            statement = connection.createStatement();
            statement.executeUpdate(query);
            statement.close();
            return 0;
        } catch (SQLException ex) {
            cancelStatement(statement, ex);
            ex.printStackTrace();
            throw new PolicyEngineException("Failed to query the database. " + ex);
        }
    }
    public String[] getPolicyId(String query) {
        // String[] polId = new String[];
        Statement statement = null;
        try {
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = statement.executeQuery(query);
            rs.last();
            String[] polId = new String[rs.getRow()];
            rs.beforeFirst();
            int counter = 0;
            while (rs.next()) {
                String polcid = rs.getString("id");
                polId[counter] = polcid;
                counter = counter + 1;
            }
            rs.close();
            statement.close();
            return polId;
        } catch(SQLException ex){
            cancelStatement(statement, ex);
            ex.printStackTrace();
            throw new PolicyEngineException("Failed to query the database. " + ex);
        }
    }

    public MetaData[] getMetaEntry(String query, String key) {
        Statement statement = null;
        try{
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = statement.executeQuery(query);
            rs.last();
            MetaData[] res = new MetaData[rs.getRow()];
            rs.beforeFirst();
            int counter = 0;
            StringBuilder builder = new StringBuilder();
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                builder.append("[");
                for (int i = 0; i < columnCount;) {
                    builder.append(rs.getString(i + 1));
                    if (++i < columnCount) builder.append(",");
                }
                builder.append("]");
            }
            String resultSetAsString = builder.toString();
            PuciLog pl = new PuciLog(key, query, resultSetAsString, "false");
            try {
                LogSieve ls = new LogSieve();
                ls.sendResults(pl);
            } catch (Exception e) { e.printStackTrace(); }
            rs.beforeFirst();
            while(rs.next()) {
                MetaData newMD = new MetaData(rs.getInt("policy_id"), rs.getString("id"), rs.getString("querier"), rs.getString("purpose"), rs.getInt("ttl"), rs.getString("origin"), rs.getString("objection"), rs.getString("sharing"), rs.getString("enforcement_action"), rs.getString("inserted_at"), rs.getInt("device_id"), rs.getString("key"));
                res[counter] = newMD;
                counter = counter + 1;
            }
            rs.close();
            statement.close();
            return res;
        } catch (SQLException ex) {
            cancelStatement(statement, ex);
            ex.printStackTrace();
            throw new PolicyEngineException("Failed to query the database. " + ex);
        }
    }

    public MallData[] getQuery(String query) {
        Statement statement = null;
        try{
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = statement.executeQuery(query);
            rs.last();
            MallData[] res = new MallData[rs.getRow()];
            rs.beforeFirst();
            int counter = 0;
            while(rs.next()) {
                MallData newMD = new MallData(rs.getString("id"), rs.getString("shop_name"), rs.getDate("obs_date"), rs.getTime("obs_time"), rs.getString("user_interest"), rs.getInt("device_id"));
                res[counter] = newMD;
                counter = counter + 1;
            }
            rs.close();
            statement.close();
            return res;
        } catch (SQLException ex) {
            cancelStatement(statement, ex);
            ex.printStackTrace();
            throw new PolicyEngineException("Failed to query the database. " + ex);
        }
        
    }
    public QueryResult runWithThread(String query, QueryResult queryResult) {

        Statement statement = null;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<QueryResult> future = null;
        try {
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            // ResultSet rs = statement.executeQuery(query);
            // ResultSetMetaData rsmd = rs.getMetaData();
            // int columnsNumber = rsmd.getColumnCount();
            // System.out.println("Column number: " + columnsNumber);
            
            // int counter = 0;
            // while (rs.next()) {
            //     // for (int i = 1; i <= columnsNumber; i++) {
            //     //     if (i > 1) System.out.print(",  ");
            //     //     String columnValue = rs.getString(i);
            //     //     System.out.print(columnValue + " " + rsmd.getColumnName(i));
            //     // }
            //     // System.out.println("");
            //     counter++;
            // }
            // System.out.println ("Row amount: " + counter);
            Executor queryExecutor = new Executor(statement, query, queryResult);
            future = executor.submit(queryExecutor);
            queryResult = future.get(timeout, TimeUnit.MILLISECONDS);
            executor.shutdown();
            return queryResult;
        } catch (SQLException | InterruptedException | ExecutionException ex) {
            cancelStatement(statement, ex);
            ex.printStackTrace();
            throw new PolicyEngineException("Failed to query the database. " + ex);
        } catch (TimeoutException ex) {
            cancelStatement(statement, ex);
            future.cancel(true);
            queryResult.setTimeTaken(PolicyConstants.MAX_DURATION);
            return queryResult;
        } finally {
            DbUtils.closeQuietly(statement);
            executor.shutdownNow();
        }
    }

    private void cancelStatement(Statement statement, Exception ex) {
        System.out.println("Cancelling the current query statement. Timeout occurred");
        try {
            statement.cancel();
        } catch (SQLException exception) {
            throw new PolicyEngineException("Calling cancel() on the Statement issued exception. Details are: " + exception);
        }
    }

    private class Executor implements  Callable<QueryResult>{

        Statement statement;
        String query;
        QueryResult queryResult;

        public Executor(Statement statement, String query, QueryResult queryResult) {
            this.statement = statement;
            this.query = query;
            this.queryResult = queryResult;
        }

        @Override
        public QueryResult call() throws Exception {
            try {
                Instant start = Instant.now();
                ResultSet rs = statement.executeQuery(query);
                Instant end = Instant.now();
                if(queryResult.getResultsCheck())
                    queryResult.setQueryResult(rs);
                int rowcount = 0;
                if (hasColumn(rs, "total")){
                    rs.next();
                    rowcount = rs.getInt(1);
                }
                else if (rs.last()) {
                    rowcount = rs.getRow();
                    rs.beforeFirst();
                }
                if(queryResult.getPathName() != null && queryResult.getFileName() != null){
                    queryResult.writeResultsToFile(rs);
                }
                queryResult.setResultCount(rowcount);
                queryResult.setTimeTaken(Duration.between(start, end));
                return queryResult;
            } catch (SQLException e) {
                System.out.println("Exception raised by : " + query);
                cancelStatement(statement, e);
                e.printStackTrace();
                throw new PolicyEngineException("Error Running Query");
            }
        }

        public boolean hasColumn(ResultSet rs, String columnName) throws SQLException {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            for (int x = 1; x <= columns; x++) {
                if (columnName.equals(rsmd.getColumnName(x))) {
                    return true;
                }
            }
            return false;
        }
    }

}
