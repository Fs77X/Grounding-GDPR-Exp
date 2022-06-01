/**
 * Copyright (c) 2010 - 2016 Yahoo! Inc., 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.yahoo.ycsb.db.JSON.MgetEntry;
import com.yahoo.ycsb.db.flavors.DBFlavor;

import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.core.type.TypeReference;
// import com.fasterxml.jackson.databind.DeserializationFeature;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.MultipartBody;
import okhttp3.ResponseBody;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 *
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 *
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type TEXT. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 */
public class JdbcDBClient extends DB {
  private int mallobsCounter = 3000000;
  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  public OkHttpClient client = new OkHttpClient().newBuilder()
          .build();

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /** The batch size for batched inserts. Set to >0 to use batching */
  public static final String DB_BATCH_SIZE = "db.batchsize";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  private List<Connection> conns;
  private boolean initialized = false;
  private Properties props;
  private int jdbcFetchSize;
  private int batchSize;
  private boolean autoCommit;
  private boolean batchUpdates;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;
  /** DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix} */
  private DBFlavor dbFlavor;

  /**
   * Ordered field information for insert and update statements.
   */
  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    String getFieldKeys() {
      return fieldKeys;
    }

    List<String> getFieldValues() {
      return fieldValues;
    }
  }

  private Connection getConnection() throws Exception{

    try {
      Connection c = null;
      Class.forName("org.postgresql.Driver");
      c = DriverManager
          .getConnection("jdbc:postgresql://127.0.0.1:5432/the_db",
              "postgres", "admin");
      // System.out.println("Opened database successfully");
      return c;
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
      return null;
    }
  }



  /**
   * For the given key, returns what shard contains data for this key.
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key.
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      if (!autoCommit) {
        conn.commit();
      }
      conn.close();
    }
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    this.jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);
    this.batchSize = getIntProperty(props, DB_BATCH_SIZE);

    this.autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
    this.batchUpdates = getBoolProperty(props, JDBC_BATCH_UPDATES, false);

    try {
      if (driver != null) {
        Class.forName(driver);
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      final String[] urlArr = urls.split(",");
      for (String url : urlArr) {
        System.out.println("Adding shard node URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, passwd);

        // Since there is no explicit commit method in the DB interface, all
        // operations should auto commit, except when explicitly told not to
        // (this is necessary in cases such as for PostgreSQL when running a
        // scan workload with fetchSize)
        conn.setAutoCommit(autoCommit);

        shardCount++;
        conns.add(conn);
      }

      System.out.println("Using shards: " + shardCount + ", batchSize:" + batchSize + ", fetchSize: " + jdbcFetchSize);

      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();

      this.dbFlavor = DBFlavor.fromJdbcUrl(urlArr[0]);
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }

    initialized = true;
  }

  @Override
  public void cleanup() throws DBException {
    if (batchSize > 0) {
      try {
        // commit un-finished batches
        for (PreparedStatement st : cachedStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        throw new DBException(e);
      }
    }

    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
      throws SQLException {
    String insert = dbFlavor.createInsertStatement(insertType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert);
    PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
      throws SQLException {
    // System.out.println("WEEW OO HEREE ESDFSDF");
    String read = dbFlavor.createReadStatement(readType, key);
    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read);
    PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) {
      return readStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheReadMetaStatement(StatementType readType, String key)
      throws SQLException {
    String read = dbFlavor.createReadMetaStatement(readType, key);
    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read);
    return readStatement;
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
      throws SQLException {
    String delete = dbFlavor.createDeleteStatement(deleteType, key);
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete);
    return deleteStatement;
  }

  private PreparedStatement createAndCacheDeleteMetaStatement(StatementType deleteType, String key)
      throws SQLException {
    String delete = dbFlavor.createDeleteMetaStatement(deleteType, key);
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete);
    return deleteStatement;
  }

  private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
      throws SQLException {
    String update = dbFlavor.createUpdateStatement(updateType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update);
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheUpdateMetaStatement(StatementType updateType, String key)
      throws SQLException {
    String update = dbFlavor.createUpdateMetaStatement(updateType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update);
    return insertStatement;
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
      throws SQLException {
    String select = dbFlavor.createScanStatement(scanType, key);
    PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select);
    if (this.jdbcFetchSize > 0) {
      scanStatement.setFetchSize(this.jdbcFetchSize);
    }
    PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (stmt == null) {
      return scanStatement;
    }
    return stmt;
  }

  public Status performRead() {
    try {
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      Random rand = new Random();
      String query = "SELECT id FROM usertable WHERE tomb = 0";
      ResultSet rs = statement.executeQuery(query);
      rs.last();
      String[] id = new String[rs.getRow()];
      rs.beforeFirst();
      int counter = 0;
      while (rs.next()) {
        String val = rs.getString("id");
        id[counter] = val;
        counter = counter + 1;
      }
      String key = id[rand.nextInt(counter)];
      query = "SELECT * from usertable WHERE id = \'" + key + "\'";
      // System.out.println(query);
      rs = statement.executeQuery(query);
      rs.last();
      rs.beforeFirst();
      counter = 0;
      StringBuilder res = new StringBuilder();
      String dId = "";
      while (rs.next()) {
        String iId = rs.getString("id");
        res.append("(" + iId).append("|");
        String shop_name = rs.getString("shop_name");
        res.append(shop_name).append("|");
        String obs_date = rs.getString("obs_date");
        res.append(obs_date).append("|");
        String obs_time = rs.getString("obs_time");
        res.append(obs_time).append("|");
        String user_interest = rs.getString("user_interest");
        res.append(user_interest).append("|");
        dId = rs.getString("device_id");
        res.append(dId).append(")");
      }
      rs.close();
      log(dId, query, res.toString());
      return Status.OK;
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  // modify
  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }
      readStatement.setString(1, key);
      // System.err.println("In Read: "+readStatement.toString());
      // ResultSet resultSet = readStatement.executeQuery();
      // if (!resultSet.next()) {
      //   resultSet.close();
      //   return Status.NOT_FOUND;
      // }
      // if (result != null && fields != null) {
      //   for (String field : fields) {
      //     String value = resultSet.getString(field);
      //     result.put(field, new StringByteIterator(value));
      //   }
      // }
      // resultSet.close();
      // System.out.println("read okchamp");
      return performRead();
      
    } catch (SQLException e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      // exception for returning ok here than error since data is random
      return Status.OK;
    }
  }

  @Override
  public Status readLog(String table, int logcount){
    try {
      String s = null;
      String query = null;
      Process p = null;
      query = "tail -n " + logcount + " /home/audit_logs/audit_dump.xm";
      p = Runtime.getRuntime().exec(query);
      BufferedReader stdInput = new BufferedReader(new
           InputStreamReader(p.getInputStream()));
      BufferedReader stdError = new BufferedReader(new
           InputStreamReader(p.getErrorStream()));
      // read the output from the command
      while ((s = stdInput.readLine()) != null) {
        System.out.println(s);
      }
      // read any errors from the attempted command
      while ((s = stdError.readLine()) != null) {
        System.out.println(s);
      }
      return Status.OK;
    } catch (IOException e) {
      System.out.println("exception happened - here's what I know: ");
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  // flag for customer or processor
  public Status performReadMeta() {
    Connection c;
    try {
      c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      Random rand = new Random();
      String query = "SELECT DISTINCT device_id, id FROM user_policy WHERE tomb = 0";
      ResultSet rs = statement.executeQuery(query);
      rs.last();
      String[] devid = new String[rs.getRow()];
      String[] id = new String[rs.getRow()];
      rs.beforeFirst();
      int counter = 0;
      while (rs.next()) {
        String val = rs.getString("device_id");
        devid[counter] = val;
        val = rs.getString("id");
        id[counter] = val;
        counter = counter + 1;
      }
      Integer sel = rand.nextInt(counter);
      String deviceid = devid[sel];
      String qkey = id[sel] + "";
      query = "SELECT * from user_policy WHERE id = \'" + qkey + "\' AND device_id = " + deviceid;
      // System.out.println(query);
      rs = statement.executeQuery(query);
      // if (!rs.next()) {
      //   rs.close();
      //   return Status.NOT_FOUND;
      // }
      String idR = "";
      String purpose = "";
      String querier = "";
      String ttl = "";
      String origin = "";
      String objection = "";
      String sharing = "";
      String enforcement_action = "";
      String inserted_at = "";
      String tomb = "";
      String device_id = "";
      while (rs.next()) {
        idR = rs.getString("id");
        purpose = rs.getString("purpose");
        querier = rs.getString("querier");
        ttl = rs.getString("ttl");
        origin = rs.getString("origin");
        objection = rs.getString("objection");
        sharing = rs.getString("sharing");
        enforcement_action = rs.getString("enforcement_action");
        inserted_at = rs.getString("inserted_at");
        tomb = rs.getString("tomb");
        device_id = rs.getString("device_id");
      }

      StringBuilder res = new StringBuilder();
      res.append(idR).append("|");
      res.append(purpose).append("|");
      res.append(querier).append("|");
      res.append(ttl).append("|");
      res.append(origin).append("|");
      res.append(objection).append("|");
      res.append(sharing).append("|");
      res.append(enforcement_action).append("|");
      res.append(inserted_at).append("|");
      res.append(tomb).append("|");
      res.append(device_id);
      // System.out.println(res.toString());
      log(deviceid, query, res.toString());

      rs.close();
      statement.close();
      c.close();
      return Status.OK;

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return Status.ERROR;
    }
   


  }

  public String getIdx() {
    try {
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      String query = "SELECT id FROM user_policy WHERE tomb = 0";
      ResultSet rs = statement.executeQuery(query);
      rs.last();
      String[] id = new String[rs.getRow()];
      rs.beforeFirst();
      int counter = 0;
      while (rs.next()) {
        String val = rs.getString("id");
        id[counter] = val;
        counter = counter + 1;
      }
      if (id.length == 0) {
        System.out.println("NOT GOOD");
      }
      Random rand = new Random();
      int idx = rand.nextInt(id.length);
      String qkey = id[idx] + "";
      rs.close();
      statement.close();
      c.close();
      return qkey;
    } catch (Exception e) {
      System.err.println(e);
      return "";
    }
  }

  public Status procReadMeta() {
    // Random rand = new Random();
    // int[] multqid  = new int[]{7, 5, 9, 12};
    // int qid = multqid[rand.nextInt(multqid.length)];
    // String id = qid + "";
    // String prop = "purpose";
    // int[] purposes  = new int[]{5, 16};
    // int ival = purposes[rand.nextInt(purposes.length)];
    // String info = "" + ival;
    try {

      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      String query = "SELECT querier, purpose FROM user_policy WHERE tomb = 0";
      ResultSet rs = statement.executeQuery(query);
      String prop = "purpose";
      rs.last();
      String[] id = new String[rs.getRow()];
      String[] purp = new String[rs.getRow()];
      rs.beforeFirst();
      int counter = 0;
      while (rs.next()) {
        String val = rs.getString("querier");
        id[counter] = val;
        val = rs.getString("purpose");
        purp[counter] = val;
        counter = counter + 1;
      }
      if (id.length == 0) {
        System.out.println("NOT GOOD");
      }
      Random rand = new Random();
      int idx = rand.nextInt(id.length);
      String qkey = id[idx] + "";
      String info = purp[idx] + "";
      rs.close();
      query = "SELECT * from user_policy WHERE querier = \'" + qkey + "\' AND " + prop + " = \'" + info + "\'";
      // System.out.println(query);
      rs = statement.executeQuery(query);
      // if (!rs.next()) {
      //   rs.close();
      //   return Status.NOT_FOUND;
      // }
      String idR = "";
      String purpose = "";
      String querier = "";
      String ttl = "";
      String origin = "";
      String objection = "";
      String sharing = "";
      String enforcement_action = "";
      String inserted_at = "";
      String tomb = "";
      String device_id = "";
      StringBuilder res = new StringBuilder();
      while (rs.next()) {
        idR = rs.getString("id");
        purpose = rs.getString("purpose");
        querier = rs.getString("querier");
        ttl = rs.getString("ttl");
        origin = rs.getString("origin");
        objection = rs.getString("objection");
        sharing = rs.getString("sharing");
        enforcement_action = rs.getString("enforcement_action");
        inserted_at = rs.getString("inserted_at");
        tomb = rs.getString("tomb");
        device_id = rs.getString("device_id");
        res.append("(" + idR).append("|");
        res.append(purpose).append("|");
        res.append(querier).append("|");
        res.append(ttl).append("|");
        res.append(origin).append("|");
        res.append(objection).append("|");
        res.append(sharing).append("|");
        res.append(enforcement_action).append("|");
        res.append(inserted_at).append("|");
        res.append(tomb).append("|");
        res.append(device_id).append(")");
      }

      
      
      // System.out.println(res.toString());
      log(qkey, query, res.toString());

      rs.close();
      statement.close();
      c.close();
      return Status.OK;

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return Status.ERROR;
    }
    
  }

  //modify
  @Override
  public Status readMeta(String tableName, int fieldnum, String cond, 
      String keymatch, Vector<HashMap<String, ByteIterator>> result, Boolean processor) {
    //TODO: No use for keyMatch whatsoever, so check if without queering for keys this will work.
    try {
      // Random rand = new Random();
      // int qid = rand.nextInt(39) + 1;
      // String[] properties = new String[]{"objection", "sharing", "purpose"};
      // String[] propVals = new String[]{"obj", "shr", "purpose"};
      // String qidS = qid + "";
      // int idx = rand.nextInt(properties.length);
      // String prop = properties[idx];
      // int ival = rand.nextInt(99) + 1;
      // String info = propVals[idx]+ival;
      HashSet<String> fields = null;
      // System.out.println("tablename: " + tableName + " keymatch " + keymatch);
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, "", getShardIndexByKey(keymatch));
      PreparedStatement readStatement = createAndCacheReadMetaStatement(type, keymatch);
      // System.out.println("readmeta query: " + readStatement.toString());
      // ResultSet resultSet = readStatement.executeQuery();
      // if (!resultSet.next()) {
      //   resultSet.close();
      //   return Status.NOT_FOUND;
      // }

//    if (result != null && fields != null) {
//        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
//        for (String field : fields) {
//          String value = resultSet.getString(field);
//          values.put(field, new StringByteIterator(value));
//        }
//        result.add(values);
//      }

      // if(result != null){
      //   HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
      //   String value = resultSet.getString("field0");
      //   values.put("field0", new StringByteIterator(value));
      //   result.add(values);
      // }
      // System.out.println("performReadMeta");
      if (processor) {
        return procReadMeta();
      } else {
        return performReadMeta();
      }
      
      // resultSet.close();
    } catch (SQLException e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, "", getShardIndexByKey(startKey));
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type, startKey);
      }
      scanStatement.setString(1, startKey);
      scanStatement.setInt(2, recordcount);
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }
          result.add(values);
        }
      }
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  public MgetEntry createMgetEntry() {
    try {
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      Random rand = new Random();
      String query = "SELECT DISTINCT id, device_id FROM user_policy WHERE tomb = 0";
      ResultSet rs = statement.executeQuery(query);
      rs.last();
      String[] devid = new String[rs.getRow()];
      String[] id = new String[rs.getRow()];
      rs.beforeFirst();
      int counter = 0;
      while (rs.next()) {
        String val = rs.getString("id");
        id[counter] = val;
        val = rs.getString("device_id");
        devid[counter] = val;
        counter = counter + 1;
      }
      Integer sel = rand.nextInt(counter);
      String deviceid = devid[sel];
      String qkey = id[sel] + "";
      rs.close();
      statement.close();
      c.close();
      return new MgetEntry(qkey, deviceid);

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }

  public Status performUpdate(String prop, String info) {
    try {
      // System.out.println("IN UPDATE");
      MgetEntry mge = createMgetEntry();
      Connection c = getConnection();
      Statement statement = c.createStatement();
      String query = "UPDATE usertable set " + prop + " = \'" + info + "\' WHERE id = \'" + mge.getId() + "\' AND device_id = " + mge.getDeviceId();
      // System.out.println("performupdate: " + query);
      int res = statement.executeUpdate(query);
      statement.close();
      c.close();
      if (res > 0) {
        log(mge.getDeviceId(), query, "update succ");
        return Status.OK;
      } else {
        System.err.println("UPDATE ERROR");
        return Status.ERROR;
      }
      // // update meta?
      // Connection c = getConnection();
      // Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      // Random rand = new Random();
      // String[] meta = {"purpose", "sharing", "origin"};
      // String selectedMeta = meta[rand.nextInt(meta.length)];
      // String query = "SELECT DISTINCT querier, " + selectedMeta + " FROM user_policy where tomb = 0";
      // ResultSet rs = statement.executeQuery(query);
      // rs.last();
      // String[] querier = new String[rs.getRow()];
      // String[] condVal = new String[rs.getRow()];
      // rs.beforeFirst();
      // int counter = 0;
      // while (rs.next()) {
      //   String q = rs.getString("querier");
      //   querier[counter] = q;
      //   condVal[counter] = rs.getString(selectedMeta);
      //   counter = counter + 1;
      // }
      // int idx = rand.nextInt(querier.length);
      // String pickedQ = querier[idx];
      // info = condVal[idx];
      // int val = rand.nextInt(100) + 1;
      // String changeVal = val + "";
      // query = "UPDATE user_policy SET " + selectedMeta + " = \'" + changeVal + "\' WHERE " + selectedMeta + " = \'" + info + "\' AND querier = \'" + pickedQ + "\' AND tomb = 0";
      // System.out.println(query);
      // int res = statement.executeUpdate(query);
      // rs.close();
      // statement.close();
      // c.close();
      // if (res != 0) {
      //   return Status.OK;
      // } else {
      //   System.out.println(query);
      //   System.out.println("FAIL");
      //   return Status.ERROR;
      // }
    } catch(Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  public Status performUpdateCustomer(String condProp, String condVal,
      String changeProp, String changeVal) {
    try {
      Random rand = new Random();
      MgetEntry mge = createMgetEntry();
      changeVal = "" + (rand.nextInt(39) + 1);
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      String query = "UPDATE user_policy SET " + condProp + " = " + changeVal + " WHERE device_id = " + mge.getDeviceId()
          + " AND id = \'" + mge.getId() + "\'";
      if (condProp.equals("device_id")) {
        System.out.println("SUSSYBAKA");
        System.out.println(query);
      }
      Integer res = statement.executeUpdate(query);
      statement.close();
      c.close();
      if (res != 0) {
        log(mge.getDeviceId(), query, res.toString());
        return Status.OK;
      } else {
        System.out.println("performupdcust: " + query);
        System.out.println("FAIL");
        return Status.ERROR;
      }
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  public Status performUpdateMeta() {
    try {
      // update meta?
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      Random rand = new Random();
      String[] meta = {"purpose", "sharing", "origin"};
      String selectedMeta = meta[rand.nextInt(meta.length)];
      String query = "SELECT DISTINCT querier, " + selectedMeta + " FROM user_policy where tomb = 0";
      ResultSet rs = statement.executeQuery(query);
      rs.last();
      String[] querier = new String[rs.getRow()];
      String[] condVal = new String[rs.getRow()];
      rs.beforeFirst();
      int counter = 0;
      while (rs.next()) {
        String q = rs.getString("querier");
        querier[counter] = q;
        condVal[counter] = rs.getString(selectedMeta);
        counter = counter + 1;
      }
      int idx = rand.nextInt(querier.length);
      String pickedQ = querier[idx];
      String info = condVal[idx];
      int val = rand.nextInt(100) + 1;
      String changeVal = val + "";
      query = "UPDATE user_policy SET " + selectedMeta + " = \'" + changeVal + "\' WHERE " + selectedMeta + " = \'" + info + "\' AND querier = \'" + pickedQ + "\' AND tomb = 0";
      Integer res = statement.executeUpdate(query);
      rs.close();
      statement.close();
      c.close();
      if (res != 0) {
        log(pickedQ, query, res.toString());
        return Status.OK;
      } else {
        System.out.println("perf updatemeta: " + query);
        System.out.println("FAIL");
        return Status.ERROR;
      }
    } catch(Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      //System.out.println("Key in update "+key);
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type, key);
      }
      int index = 1;
      for (String value: fieldInfo.getFieldValues()) {
        updateStatement.setString(index++, value);
      }
      updateStatement.setString(index, key);
      // int result = updateStatement.executeUpdate();
      // if (result == 1) {
      //   return Status.OK;
      // }
      Random rand = new Random();
      String prop = "shop_name";
      String info = "store " + (rand.nextInt(99) + 1);
      return performUpdate(prop, info);
      // return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }
  public String propChange(String prop) {
    switch (prop) {
    case "SRC":
      return "origin";
    case "OBJ":
      return "objection";
    case "PUR":
      return "purpose";
    case "SHR":
      return "sharing";
    case "TTL":
      return "ttl";
    default:
      return "purpose";
    }
  }

  @Override
  public Status updateMeta(String table, int fieldnum, String condition, 
      String keymatch, String fieldname, String metadatavalue, String condProp, Boolean customer) {
    try{
      StatementType type = new StatementType(StatementType.Type.UPDATE, table,
          1, "", getShardIndexByKey(keymatch));
      //System.out.println(type.getFieldString());
      PreparedStatement updateStatement = createAndCacheUpdateMetaStatement(type, keymatch);
      //updateStatement.setString(1,keymatch);
      // int result = updateStatement.executeUpdate();
      condProp = propChange(condProp);
      String condVal = condition;
      String changeProp = propChange(fieldname);
      String changeVal = metadatavalue;
      // String changeVal = metadatavalue;
      
      // System.out.println("PERFORM UPDATEMETA");
      if (customer) {
        return performUpdateCustomer(condProp, condVal, changeProp, changeVal);
      } else {
        return performUpdateMeta();
      }
      
      //System.err.println("UpdateMeta statement "+updateStatement+" Result "+result);
      // return Status.OK;
    } catch(SQLException e) {
      System.err.println("Error in processing update to table: " + table + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type, key);
      }
      //System.err.println("In insert: "+insertStatement.toString());
      insertStatement.setString(1, key);
      int index = 2;
      for (String value: fieldInfo.getFieldValues()) {
        insertStatement.setString(index++, value);
      }
      // Using the batch insert API
      if (batchUpdates) {
        insertStatement.addBatch();
        // Check for a sane batch size
        if (batchSize > 0) {
          // Commit the batch after it grows beyond the configured size
          if (++numRowsInBatch % batchSize == 0) {
            int[] results = insertStatement.executeBatch();
            for (int r : results) {
              // Acceptable values are 1 and SUCCESS_NO_INFO (-2) from reWriteBatchedInserts=true
              if (r != 1 && r != -2) { 
                return Status.ERROR;
              }
            }
            // If autoCommit is off, make sure we commit the batch
            if (!autoCommit) {
              getShardConnectionByKey(key).commit();
            }
            return Status.OK;
          } // else, the default value of -1 or a nonsense. Treat it as an infinitely large batch.
        } // else, we let the batch accumulate
        // Added element to the batch, potentially committing the batch too.
        return Status.BATCHED_OK;
      } else {
        // Normal update
        int result = insertStatement.executeUpdate();
        // If we are not autoCommit, we might have to commit now
        if (!autoCommit) {
          // Let updates be batcher locally
          if (batchSize > 0) {
            if (++numRowsInBatch % batchSize == 0) {
              // Send the batch of updates
              getShardConnectionByKey(key).commit();
            }
            // uhh
            return Status.OK;
          } else {
            // Commit each update
            getShardConnectionByKey(key).commit();
          }
        }
        if (result == 1) {
          return Status.OK;
        }
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  public void log(String querier, String query, String results) {
    try {
      // MediaType mediaType = MediaType.parse("text/plain");
      RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
          .addFormDataPart("querier", querier)
          .addFormDataPart("query", query)
          .addFormDataPart("result", results)
          .build();
      Request request = new Request.Builder()
          .url("http://localhost:8000/add_log/")
          .method("POST", body)
          .build();
      Response response = client.newCall(request).execute();
      if (response.code() != 201) {
        ResponseBody boi = response.body();
        boi.close();
        return;
      }
      ResponseBody boi = response.body();
      boi.close();
    } catch (Exception e) {
      System.err.println(e);
    } 
  }

  public Status performDelete() {
    try {
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      Random rand = new Random();
      String query = "SELECT DISTINCT device_id, id FROM user_policy WHERE tomb = 0";
      ResultSet rs = statement.executeQuery(query);
      rs.last();
      String[] devid = new String[rs.getRow()];
      String[] id = new String[rs.getRow()];
      rs.beforeFirst();
      int counter = 0;
      while (rs.next()) {
        String val = rs.getString("device_id");
        devid[counter] = val;
        val = rs.getString("id");
        id[counter] = val;
        counter = counter + 1;
      }
      Integer sel = rand.nextInt(counter);
      String deviceid = devid[sel];
      String qkey = id[sel] + "";
      // query = "SELECT id FROM user_policy WHERE device_id = " + deviceid + " AND tomb = 0";
      // rs = statement.executeQuery(query);
      // rs.last();
      
      // rs.beforeFirst();
      // counter = 0;
      // while (rs.next()) {
      //   String val = rs.getString("id");
      //   id[counter] = val;
      //   counter = counter + 1;
      // }
      // if (id.length == 0) {
      //   System.out.println("NOT GOOD");
      // }
      
      rs.close();
      // query = "DELETE from mall_observation WHERE id = \'" + qkey + "\'";
      // // System.out.println(query);
      // int res = statement.executeUpdate(query);
      // if (res < 0) {
      //   System.out.println(res);
      //   System.out.println("ERR DELETING data for: " + qkey);
      //   return Status.ERROR;
      // }
      query = "UPDATE user_policy set tomb = 1 WHERE id = \'" + qkey + "\' AND device_id = " + deviceid;
      // System.out.println(query);
      Integer res = statement.executeUpdate(query);
      if (res < 0) {
        System.out.println(res);
        System.out.println("ERR DELETING METADA for: " + qkey);
        return Status.ERROR;
      }
      statement.close();
      c.close();
      log(deviceid, qkey, res.toString());
      return Status.OK;
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return Status.ERROR;
    }
  }



  @Override
  public Status delete(String tableName, String key) {
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      deleteStatement.setString(1, key);
      // int result = deleteStatement.executeUpdate();
      //System.err.println("Delete Jdbc key "+key+ "result "+ result);
      // if (result == 1) {
      //   return Status.OK;
      // }
      return performDelete();
      
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status deleteMeta(String table, int fieldnum, String condition, String keymatch) {
    try{
      StatementType type = new StatementType(StatementType.Type.DELETE, table, 1, "", getShardIndexByKey(keymatch));
      PreparedStatement deleteStatement = createAndCacheDeleteMetaStatement(type, keymatch);
      int result = deleteStatement.executeUpdate();
      //System.err.println("DeleteMeta Jdbc key "+keymatch+ "result "+ result);
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + table + e);
      return Status.ERROR;
    }
  }

  private OrderedFieldInfo getFieldInfo(Map<String, ByteIterator> values) {
    String fieldKeys = "";
    List<String> fieldValues = new ArrayList<>();
    int count = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldKeys += entry.getKey();
      if (count < values.size() - 1) {
        fieldKeys += ",";
      }
      fieldValues.add(count, entry.getValue().toString());
      count++;
    }

    return new OrderedFieldInfo(fieldKeys, fieldValues);
  }

  @Override
  public Status verifyTTL(String table, long recordcount) {
    return Status.OK;
  }

  public MaddObj generateData() {
    // mallData
    String mdId = "o" + mallobsCounter;
    mallobsCounter = mallobsCounter + 1;
    Random rand = new Random();
    String shopName = "store " + (rand.nextInt(99) + 1);
    long now = System.currentTimeMillis();
    Time obs_time = new Time(now);
    Date obs_date = new Date(now);
    String[] userInterest = {"", "shoes", "fastfood", "cars", "planes"};
    String uInterest = userInterest[rand.nextInt(userInterest.length)];
    int device_id =  rand.nextInt(2000) + 1;
    MallData mallData = new MallData(mdId, shopName, obs_date, obs_time, uInterest, device_id);

    // metadata
    Instant instant = Instant.ofEpochMilli(now);
    long res = instant.getEpochSecond();
    int ttl = (int)res + rand.nextInt(4000) + 30;
    String uuid = UUID.randomUUID().toString();
    int q = rand.nextInt(100) + 1;
    String querier = q + "";
    String purpose = (rand.nextInt(100) + 1) + "";
    String origin = (rand.nextInt(100) + 1) + "";
    String objection = (rand.nextInt(100) + 1) + "";
    String sharing = (rand.nextInt(100) + 1) + "";
    String enforcement = "allow";
    Timestamp ts = new Timestamp(now);
    String timeStamp = ts.toString();
    String key = mdId;
    MetaData mData = new MetaData(uuid, querier, purpose, ttl, origin, objection, sharing, enforcement, timeStamp, device_id, key);
    return new MaddObj(mallData, mData);
  }

  @Override
  public Status insertTTL(String table, String key,
                         Map<String, ByteIterator> values, int ttl) {
    // System.out.println("inside inserttl");
    try{
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      MaddObj newObj = generateData();
      Connection c = getConnection();
      Statement statement = c.createStatement();
      StringBuilder sb = new StringBuilder("INSERT INTO usertable(id, shop_name, obs_date, obs_time, ");
      sb.append("user_interest, device_id, tomb) VALUES(");
      sb.append("\'").append(newObj.getMallData().getId()).append("\', ");
      sb.append("\'").append(newObj.getMallData().getShopName()).append("\', ");
      sb.append("\'").append(newObj.getMallData().getObsDate()).append("\', ");
      sb.append("\'").append(newObj.getMallData().getObsTime()).append("\', ");
      sb.append("\'").append(newObj.getMallData().getUserInterest()).append("\', ");
      sb.append("\'").append(newObj.getMallData().getDeviceID()).append("\', ");
      sb.append("\'").append(0).append("\')");
      statement.executeUpdate(sb.toString());
      log(newObj.getMallData().getDeviceID().toString(), sb.toString(), "INSERT USERDATA SUCC");
      // StringBuilder sbM = new StringBuilder("INSERT INTO user_policy(id, purpose, querier, ttl, origin, objection, sharing");
      // sbM.append(", enforcement_action, inserted_at, tomb, device_id) VALUES(");
      // sbM.append("\'").append(newObj.getMallData().getId()).append("\', ");
      // sbM.append("\'").append(newObj.getMetaData().getPurpose()).append("\', ");
      // sbM.append("\'").append(newObj.getMetaData().getQuerier()).append("\', ");
      // sbM.append("\'").append(newObj.getMetaData().getTtl()).append("\', ");
      // sbM.append("\'").append(newObj.getMetaData().getOrigin()).append("\', ");
      // sbM.append("\'").append(newObj.getMetaData().getObjection()).append("\', ");
      // sbM.append("\'").append(newObj.getMetaData().getSharing()).append("\', ");
      // sbM.append("\'").append(newObj.getMetaData().getEnforcementAction()).append("\', ");
      // sbM.append("\'").append(newObj.getMetaData().getInsertedAt()).append("\', ");
      // sbM.append("\'").append(0).append("\', ");
      // sbM.append("\'").append(newObj.getMallData().getDeviceID()).append("\')");
      // statement.executeUpdate(sbM.toString());
      statement.close();
      c.close();
      // log(newObj.getMallData().getDeviceID().toString(), sbM.toString(), "INSERT USERMETA SUCC");
    } catch (Exception e) {
      System.err.println("Error in processing insert to table: " + table + e);
      return Status.ERROR;
    }
  
    return Status.OK;
  }
}
