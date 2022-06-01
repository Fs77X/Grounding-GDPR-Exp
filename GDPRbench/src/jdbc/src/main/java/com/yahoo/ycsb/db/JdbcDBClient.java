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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.sql.Time;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;

import com.yahoo.ycsb.db.JSON.MaddObj;
import com.yahoo.ycsb.db.JSON.MallData;
import com.yahoo.ycsb.db.JSON.MetaData;
import com.yahoo.ycsb.db.JSON.MgetEntry;
import com.yahoo.ycsb.db.JSON.MgetObj;
import com.yahoo.ycsb.db.JSON.MmetaController;
import com.yahoo.ycsb.db.flavors.DBFlavor;
import java.util.Random;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
// import com.fasterxml.jackson.core.type.TypeReference;
// import com.fasterxml.jackson.databind.DeserializationFeature;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.MultipartBody;
import okhttp3.ResponseBody;
import java.util.UUID;

// import java.util.regex.Matcher;
// import java.util.regex.Pattern;

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
  private int userPCounter = 300000;

  public OkHttpClient client = new OkHttpClient().newBuilder().build();
  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

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
  /**
   * DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix}
   */
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

  private Connection getConnection() throws Exception{

    try {
      Connection c = null;
      Class.forName("org.postgresql.Driver");
      c = DriverManager
          .getConnection("jdbc:postgresql://127.0.0.1:5432/sieve",
              "postgres", "admin");
      // System.out.println("Opened database successfully");
      return c;
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
      return null;
    }
  }

  public MgetEntry createMgetEntry() {
    try {
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      Random rand = new Random();
      // String query = "SELECT DISTINCT device_id FROM mall_observation";
      // ResultSet rs = statement.executeQuery(query);
      // rs.last();
      // String[] devid = new String[rs.getRow()];
      // rs.beforeFirst();
      // int counter = 0;
      // while (rs.next()) {
      //   String val = rs.getString("device_id");
      //   devid[counter] = val;
      //   counter = counter + 1;
      // }
      // String deviceid = devid[rand.nextInt(counter)];
      String query = "SELECT id FROM mall_observation";
      ResultSet rs = statement.executeQuery(query);
      rs.last();
      
      String[] id = new String[rs.getRow()];
      rs.beforeFirst();
      Integer counter = 0;
      while (rs.next()) {
        String val = rs.getString("id");
        id[counter] = val;
        counter = counter + 1;
      }
      if (id.length == 0) {
        System.out.println("NOT GOOD");
      }
      // Random rand2 = new Random();
      int idx = rand.nextInt(id.length);
      // System.out.println(idx);
      String qkey = id[idx] + "";
      rs.close();
      statement.close();
      c.close();
      return new MgetEntry(qkey, "");

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
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

  /**
   * Returns parsed int value from the properties if set, otherwise returns -1.
   */
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

  /**
   * Returns parsed boolean value from the properties if set, otherwise returns
   * defaultVal.
   */
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

  public Status actualRead(String key) {
    try {
      // Pattern p = Pattern.compile("\\d+");
      // Matcher m = p.matcher(key);
      // while(m.find()) {
      // key = "o" + m.group();
      // }
      // Random rand = new Random();
      // int keyNum = rand.nextInt((999999 - 10000)) + 10000;
      // key = "o" + keyNum;
      // // 999999 10000
      MgetEntry mge = createMgetEntry();
      // System.out.println("http://localhost:5344/mget_entry/" + mge.getDeviceId() + "/" + mge.getId());
      Request request = new Request.Builder()
          .url("http://localhost:5344/sieve/mget_entry/" + mge.getId())
          .method("GET", null)
          .build();
      Response response = client.newCall(request).execute();
      // System.out.println("response code in mget_entry: " + response.code());
      ResponseBody boi = response.body();
      boi.close();
      // we have found that sometimes data gets deleted or expires before it can be
      // read due to ttl
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }

  }

  // work on read
  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    // System.out.println("we in the read");
    try {
      Status resRead = actualRead(key);
      return resRead;
    } catch (Exception e) {
      System.err.println(key + ": " + e);
      return Status.ERROR;
    }
  }

  public Status getLog(String logCount) {
    try {
      Request request = new Request.Builder()
          .url("http://localhost:8080/getLogs/" + logCount)
          .method("GET", null)
          .addHeader("Content-Type", "application/json")
          .build();
      Response response = client.newCall(request).execute();
      ResponseBody boi = response.body();
      ObjectMapper mapper = new ObjectMapper();
      List<String> logList = mapper.readValue(boi.string(), List.class);
      for (String log : logList) {
        System.out.println(log);
      }
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status readLog(String table, int logcount) {
    try {
      System.out.println(logcount);
      Status res = getLog(String.valueOf(logcount));
      return Status.OK;
    } catch (Exception e) {
      System.out.println("exception happened - here's what I know: ");
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  public Status readProcMeta(String cond, String keymatch) {
    // Random rand = new Random();
    // int qid = rand.nextInt(39) + 1;
    // String id = qid + "";
    // // System.out.println("IN ACTUAL RMD");
    // String prop = "purpose";
    // int ival = rand.nextInt(99) + 1;
    // String info = ""+ival;
    // MgetObj mObj = new MgetObj(id, prop, info);
 
    // if (cond.equals("USR")) {
    //   id = "[\"" + keymatch.replace("user", "key") + "\"]";
    // } else {
    //   purpose = "[{\"prop\": \"" + propChange(cond) + "\", \"info\": \"" + keymatch + "\"}]";
    //   int numid = (int) Math.random() * (40) + 1;
    //   id = "[" + "\"" + Integer.toString(numid) + "\"]";
    // }
    // System.out.println(id);
    // System.out.println(purpose);
    try {
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      String query = "SELECT querier, purpose FROM user_policy";
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
      statement.close();
      c.close();
      MgetObj mObj = new MgetObj(qkey, prop, info);
      ObjectMapper mapper = new ObjectMapper();
      String jsonString = "";
      try {
        jsonString = mapper.writeValueAsString(mObj);
      } catch (JsonProcessingException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      // System.out.println(jsonString);
      MediaType mediaType = MediaType.parse("application/json");
      RequestBody body = RequestBody.create(mediaType,
          jsonString);
      Request request = new Request.Builder()
          .url("http://localhost:5344/sieve/mget_obj/")
          .method("POST", body)
          .addHeader("Content-Type", "application/json")
          .build();
      Response response = client.newCall(request).execute();
      // System.out.println("RESPONSE CODE IN RMD: " + response.code());
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  public Status actualReadMeta(String cond, String keymatch) {
    MgetEntry mge = createMgetEntry();
    try {
      OkHttpClient client = new OkHttpClient().newBuilder()
          .build();
      Request request = new Request.Builder()
          .url("http://localhost:5344/sieve/mget_metaEntry/" + mge.getDeviceId() + "/" + mge.getId())
          .method("GET", null)
          .build();
      Response response = client.newCall(request).execute();
      // System.out.println("RESPONSE CODE IN RMD: " + response.code());
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status readMeta(String tableName, int fieldnum, String cond,
      String keymatch, Vector<HashMap<String, ByteIterator>> result, Boolean processor) {
    // TODO: No use for keyMatch whatsoever, so check if without queering for keys
    // this will work.
    try {
      if (processor) {
        return readProcMeta(cond, keymatch);
      } else {
        return actualReadMeta(cond, keymatch);
      }
    } catch (Exception e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    // System.out.println("we in the scan");
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

  public Status actualUpdate(String key, Map<String, ByteIterator> values) {
    try {
      Random rand = new Random();
      MgetEntry mge = createMgetEntry();
      String prop = "shop_name";
      String info = "store " + (rand.nextInt(99) + 1);
      MgetObj mObj = new MgetObj(mge.getDeviceId(), prop, info);
      ObjectMapper mapper = new ObjectMapper();
      String jsonString = "";
      try {
        jsonString = mapper.writeValueAsString(mObj);
      } catch (JsonProcessingException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      MediaType mediaType = MediaType.parse("application/json");
      RequestBody body = RequestBody.create(mediaType,
          jsonString);
      Request request = new Request.Builder()
          .url("http://localhost:5344/sieve/mmodify_obj/" + mge.getDeviceId() + "/" + mge.getId())
          .method("PUT", body)
          .build();
      Response response = client.newCall(request).execute();
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  public Status updateIndividualMeta(String key, String prop, String info) {
    try {
      MediaType mediaType = MediaType.parse("text/plain");
      RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
          .addFormDataPart("property", prop)
          .addFormDataPart("info", info)
          .build();
      Request request = new Request.Builder()
          .url("http://localhost:5344/sieve/mmodify_metaobj/" + key)
          .method("PUT", body)
          .build();
      Response response = client.newCall(request).execute();
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  // change this.
  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      // System.out.println("Key in update " + key);
      // System.out.println("Values in update" + values);
      Status updateObj = actualUpdate(key, values);
      if (!updateObj.isOk()) {
        return Status.ERROR;
      }
      // for (Map.Entry m : values.entrySet()) {
      //   if (!m.getKey().equals("USR")) {
      //     Status updateMeta = updateIndividualMeta(key, propChange("" + m.getKey()), "" + m.getValue());
      //     if (!updateMeta.isOk()) {
      //       return Status.ERROR;
      //     }
      //   }
      // }
      return Status.OK;
      // int numFields = values.size();
      // OrderedFieldInfo fieldInfo = getFieldInfo(values);
      // // System.out.println("fieldInfo: " + fieldInfo.getFieldValues().get(0));
      // StatementType type = new StatementType(StatementType.Type.UPDATE, tableName,
      // numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      // PreparedStatement updateStatement = cachedStatements.get(type);
      // if (updateStatement == null) {
      // updateStatement = createAndCacheUpdateStatement(type, key);
      // }
      // int index = 1;
      // for (String value: fieldInfo.getFieldValues()) {
      // updateStatement.setString(index++, value);
      // }
      // updateStatement.setString(index, key);
      // int result = updateStatement.executeUpdate();
      // if (result == 1) {
      // return Status.OK;
      // }
      // return Status.UNEXPECTED_STATE;
    } catch (Exception e) {
      System.err.println(e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  public MmetaController generateMMController() {
    try {
      Connection c = getConnection();
      Statement statement = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      Random rand = new Random();
      String[] meta = {"purpose", "sharing", "origin"};
      String selectedMeta = meta[rand.nextInt(meta.length)];
      String query = "SELECT DISTINCT querier, " + selectedMeta + " FROM user_policy";
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
      rs.close();
      statement.close();
      c.close();
      return new MmetaController(selectedMeta, info, pickedQ, changeVal);

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }

  }

  public Status actualupdateMeta(String condProp, String condVal,
      String changeProp, String changeVal) {
    // mmodcond_metaObj
    try {
      // Random rand = new Random();
      // MgetEntry mge = createMgetEntry();
      // changeVal = "" + (rand.nextInt(39) + 1);
      // MgetObj mObj = new MgetObj(mge.getDeviceId(), condProp, changeVal);
      MmetaController mc = generateMMController();
      ObjectMapper mapper = new ObjectMapper();
      String jsonString = "";
      try {
        jsonString = mapper.writeValueAsString(mc);
      } catch (JsonProcessingException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      // System.out.println(jsonString);
      MediaType mediaType = MediaType.parse("application/json");
      RequestBody body = RequestBody.create(mediaType, jsonString);
      Request request = new Request.Builder()
          .url("http://localhost:5344/sieve/mmodify_metaController")
          .method("PUT", body)
          .build();
      Response response = client.newCall(request).execute();
      // System.out.println(response);
      if (response.code() != 200) {
        return Status.ERROR;
      }
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  public String propChange(String prop) {
    switch (prop) {
    case "DEC":
      return "adm";
    case "USR":
      return "device_id";
    case "SRC":
      return "origin";
    case "OBJ":
      return "objection";
    case "CAT":
      return "cat";
    case "ACL":
      return "acl";
    case "PUR":
      return "purpose";
    case "SHR":
      return "sharing";
    case "TTL":
      return "ttl";
    default:
      return "";
    }
  }

  @Override
  public Status updateMeta(String table, int fieldnum, String condition,
      String keymatch, String fieldname, String metadatavalue, String condProp) {
    try {
      // just quit because that shouldn't be allowed unless we're changing usernames
      // also USR isn't considered metadata in our case
      if (fieldname.equals("USR") || fieldname.equals("Data")) {
        return Status.OK;
      }
      condProp = propChange(condProp);
      String condVal = condition;
      String changeProp = propChange(fieldname);
      String changeVal = metadatavalue;
      // System.out.println("condProp: " + condProp + " condVal " + condition
      // + " changeProp " + fieldname + " changeVal " + metadatavalue);
      StatementType type = new StatementType(StatementType.Type.UPDATE, table,
          1, "", getShardIndexByKey(keymatch));
      PreparedStatement updateStatement = createAndCacheUpdateMetaStatement(type, keymatch);
      // updateStatement.setString(1,keymatch);
      // int result = updateStatement.executeUpdate();
      Status res = actualupdateMeta(condProp, condVal, changeProp, changeVal);
      // System.out.println(res);
      // System.err.println("UpdateMeta statement "+updateStatement+" Result
      // "+result);
      return Status.OK;
    } catch (SQLException e) {
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
      // System.err.println("In insert: "+insertStatement.toString());
      insertStatement.setString(1, key);
      int index = 2;
      for (String value : fieldInfo.getFieldValues()) {
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
              // Acceptable values are 1 and SUCCESS_NO_INFO (-2) from
              // reWriteBatchedInserts=true
              if (r != 1 && r != -2) {
                return Status.ERROR;
              }
            }
            // If autoCommit is off, make sure we commit the batch
            if (!autoCommit) {
              getShardConnectionByKey(key).commit();
            }
            return Status.OK;
          } // else, the default value of -1 or a nonsense. Treat it as an infinitely large
            // batch.
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

  public Status actualDeleteMeta(String key) {
    try {
      Request request = new Request.Builder()
          .url("http://localhost:8080/mdelete_UserMetaobj/" + key)
          .method("DELETE", null)
          .build();
      Response response = client.newCall(request).execute();
      // System.out.println(response.code());
      if (response.code() != 200) {
        return Status.ERROR;
      }
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  // modify
  public Status actualDelete(String key) {
    try {
      MgetEntry mge = createMgetEntry();
      Request request = new Request.Builder()
          .url("http://localhost:5344/sieve/mdelete_obj/" + mge.getDeviceId() + "/" + mge.getId())
          .method("DELETE", null)
          .build();
      Response response = client.newCall(request).execute();
      // System.out.println(response.code());
      if (response.code() != 200) {
        return Status.ERROR;
      }
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e);
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
      // System.out.println("delete Obj");
      // int result = deleteStatement.executeUpdate();
      int result = 1; // bypass postgres failure
      // System.err.println("Delete Jdbc key "+key+ "result "+ result);
      if (result == 1) {
        Status del = actualDelete(key);
        // Status delmeta = actualDeleteMeta(key);
        // System.out.println(del);
        // System.out.println(delmeta);
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status deleteMeta(String table, int fieldnum, String condition, String keymatch) {
    // System.out.println("we in the deleteMeta");
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, table, 1, "", getShardIndexByKey(keymatch));
      PreparedStatement deleteStatement = createAndCacheDeleteMetaStatement(type, keymatch);
      int result = deleteStatement.executeUpdate();
      // System.err.println("DeleteMeta Jdbc key "+keymatch+ "result "+ result);
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + table + e);
      return Status.ERROR;
    }
  }

  private OrderedFieldInfo getFieldInfo(Map<String, ByteIterator> values) {
    String fieldKeys = "";
    // System.out.println("we in the getfieldinfo");
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
    // System.out.println(fieldKeys);
    // System.out.println(fieldValues.get(0));
    return new OrderedFieldInfo(fieldKeys, fieldValues);
  }

  public long getKeys() {
    try {
      Request request = new Request.Builder()
          .url("http://localhost:8080/getKeyCount")
          .method("GET", null)
          .build();
      Response response = client.newCall(request).execute();
      // System.out.println(response.body().string());
      ResponseBody boi = response.body();
      ObjectMapper mapper = new ObjectMapper();
      // System.out.println(boi.string());
      // System.out.println("reach b4b4here");
      TTLBlob count = mapper.readValue(boi.string(), TTLBlob.class);
      // System.out.println("reach b4here");
      boi.close();
      // System.out.println("reach here");
      return Long.parseLong(count.getIDCount());
    } catch (Exception e) {
      // System.out.println(e);
      return -1;
    }

  }

  @Override
  public Status verifyTTL(String table, long recordcount) {
    System.out.println("we in the verifyttl");
    System.out.println(table + " " + recordcount);
    long keys = getKeys();
    recordcount++;
    System.out.println("Keys in vttl: " + keys);
    while (keys > recordcount) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        System.out.println(e);
      }
      keys = getKeys();
    }
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
    // Instant instant = Instant.ofEpochMilli(now);
    // long res = instant.getEpochSecond();
    int ttl = 0;
    int polid = 0;
    // userPCounter = userPCounter + 1;
    String uuid = "q";
    // int q = 0;
    String querier = "q";
    String purpose = "q";
    String origin = "q";
    String objection = "q";
    String sharing = "q";
    String enforcement = "allow";
    // Timestamp ts = new Timestamp(now);
    String timeStamp = "q";
    String key = "q";
    MetaData mData = new MetaData(polid, uuid, querier, purpose, ttl, origin, objection, sharing, enforcement, timeStamp, device_id, key);
    return new MaddObj(mallData, mData);
  }

  private Status insertEntry(String key, String value, String name) {
    try {
      MaddObj newObj = generateData();
      MediaType mediaType = MediaType.parse("application/json");
      ObjectMapper mapper = new ObjectMapper();
      String jsonString = "";
      try {
        jsonString = mapper.writeValueAsString(newObj);
      } catch (JsonProcessingException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      // System.out.println(jsonString);
      RequestBody body = RequestBody.create(mediaType, jsonString);
      Request request = new Request.Builder()
          .url("http://localhost:5344/sieve/madd_obj/" + newObj.getMallData().getDeviceID())
          .method("POST", body)
          .build();
      Response response = client.newCall(request).execute();
      if (response.code() != 201) {
        ResponseBody boi = response.body();
        boi.close();
        return Status.ERROR;
      }
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  private Status insertMeta(String key, OrderedFieldInfo value) {
    try {
      MediaType mediaType = MediaType.parse("application/json");
      RequestBody body = RequestBody.create(mediaType, "{\r\n    \"TTL\":\"" + value.getFieldValues().get(9) +
          "\",\r\n    \"purpose\": \"" + value.getFieldValues().get(7) +
          "\",\r\n    \"adm\": \"" + value.getFieldValues().get(0) +
          "\",\r\n    \"origin\": \"" + value.getFieldValues().get(2) +
          "\",\r\n    \"objection\": \"" + value.getFieldValues().get(3) +
          "\",\r\n    \"sharing\": \"" + value.getFieldValues().get(8) +
          "\",\r\n    \"cat\": \"" + value.getFieldValues().get(4) +
          "\",\r\n    \"acl\": \"" + value.getFieldValues().get(5) + "\"\r\n}");
      Request request = new Request.Builder()
          .url("http://localhost:8080/madd_metaobj/" + key)
          .method("POST", body)
          .build();
      Response response = client.newCall(request).execute();
      // System.out.println(response);
      if (response.code() != 201) {
        return Status.ERROR;
      }
      ResponseBody boi = response.body();
      boi.close();
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insertTTL(String table, String key,
      Map<String, ByteIterator> values, int ttl) {
    // System.out.println(table + " " + key);
    // System.out.println(ttl);
    // System.out.println(values);
    // create a client
    try {
      OrderedFieldInfo payload = getFieldInfo(values);
      // System.out.println(payload.getFieldValues().get(6));
      Status insertStatus = insertEntry(key, payload.getFieldValues().get(6), payload.getFieldValues().get(1));
      if (insertStatus == null || !insertStatus.isOk()) {
        return Status.ERROR;
      }
      // Status metaStatus = insertMeta(key, payload);
      // if (metaStatus == null || !metaStatus.isOk()) {
      //   return Status.ERROR;
      // }
      // OkHttpClient client = new OkHttpClient().newBuilder()
      // .build();
      // MediaType mediaType = MediaType.parse("text/plain");
      // RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
      // .addFormDataPart("id", key)
      // .addFormDataPart("name", "Bol")
      // .addFormDataPart("gpa", payload.getFieldValues().get(6))
      // .build();
      // Request request = new Request.Builder()
      // .url("http://localhost:8000/madd_obj/")
      // .method("POST", body)
      // .build();
      // Response response = client.newCall(request).execute();
      // System.out.println(response.code());
      // if (response.code() != 201) {
      // return Status.ERROR;
      // }
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }
}
