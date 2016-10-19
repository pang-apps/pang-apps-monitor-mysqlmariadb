/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2015 Preversoft
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.pangdata.apps.monitor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pangdata.sdk.Pang;
import com.pangdata.sdk.mqtt.PangMqtt;
import com.pangdata.sdk.util.PangProperties;

// http://www.fromdual.com/innodb-variables-and-status-explained
// http://dba.stackexchange.com/questions/56494/making-sense-of-innodb-buffer-pool-stats
public class MysqlMariaDBMonitor {
  private static final Logger logger = LoggerFactory.getLogger(MysqlMariaDBMonitor.class);

  // pang-a-01.status.queries_per_second
  // pang-a-01.jdbc.username=pangdata
//pang-a-01.status.queries_per_second
  private static final String TRAFFIC_OUT = "traffic_out";
  private static final String TRAFFIC_IN = "traffic_in";
  private static final String QUERIES_PER_SECOND = "queries_per_second";
  private static final String BUFFER_USAGE = "buffer_usage";
  private static final String READS_PER_SECOND = "reads_per_second";
  private static final String WRITES_PER_SECOND = "writes_per_second";

  private static final String decimalPattern = "([0-9]*)\\.([0-9]*)";
  private static final String STATUS = "SHOW GLOBAL STATUS";

  private static String JDBC_DRIVER;
  private static String DB_URL;
  private static String USER;
  private static String PASS;
  private static String PREFIX;

  private static long queries;
  private static int writes;
  private static int reads;
  private static long bytes_in;
  private static long bytes_out;
  private static long time;

  private static int total_page;
  private static int data_page;

  private static Connection conn = null;
  private static Statement stmt = null;

  public static void main(String[] args) throws Exception {
    // Prever must be initialized first to use prever.properties by PangProperties
    final Pang prever = new PangMqtt();

    JDBC_DRIVER = (String) PangProperties.getProperty("jdbc.driverClassName");
    DB_URL = (String) PangProperties.getProperty("jdbc.url");
    USER = (String) PangProperties.getProperty("jdbc.username");
    PASS = (String) PangProperties.getProperty("jdbc.password");
    PREFIX = (String) PangProperties.getProperty("pang.prefix");
    
    if(PREFIX == null || PREFIX.trim().isEmpty()) {
      logger.error("Prefix doesn't defined. Please set prefix configuration on conf/pang.properties.");
      return;
    }

    final Map<String, String> fields = extractFields();

    final boolean queries_per_second = fields.containsKey(QUERIES_PER_SECOND);
    final boolean traffic_in = fields.containsKey(TRAFFIC_IN);
    final boolean traffic_out = fields.containsKey(TRAFFIC_OUT);
    final boolean buffer_usage = fields.containsKey(BUFFER_USAGE);
    final boolean writes_per_second = fields.containsKey(WRITES_PER_SECOND);
    final boolean reads_per_second = fields.containsKey(READS_PER_SECOND);

    getConn();

    long period = PangProperties.getPeriod();
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        long rtime = 0;
        try {
          Connection conn = getConn();

          stmt = conn.createStatement();

          rtime = System.currentTimeMillis();
          ResultSet rs = stmt.executeQuery(STATUS);

          Map<String, Object> values = new HashMap<String, Object>();

          total_page = -1;
          data_page = -1;

          while (rs != null && rs.next()) {
            String field = rs.getString(1).toLowerCase();
            String value = rs.getString(2);

            if (queries_per_second && field.equals("queries")) {
              getQueriesPerSecond(queries_per_second, rtime, values, field, value);
            } else if (traffic_in && field.equals("bytes_received")) {
              getTrafficIn(traffic_in, rtime, values, field, value);
            } else if (traffic_out && field.equals("bytes_sent")) {
              getTrafficOut(rtime, values, field, value);
            } else if (writes_per_second && field.equals("innodb_data_writes")) {
              getWritesPerSecond(rtime, values, field, value);
            } else if (reads_per_second && field.equals("innodb_data_reads")) {
              getReadsPerSecond(rtime, values, field, value);
            } else if (buffer_usage
                && (field.equals("innodb_buffer_pool_pages_data") || field
                    .equals("innodb_buffer_pool_pages_total"))) {
              getBufferUsage(field, value);
              if (total_page > -1 && data_page > -1) {
                values.put(BUFFER_USAGE, rountTo2decimal(data_page * 100.0 / total_page));
              }
            }

            if (fields.containsKey(field)) {
              boolean isdouble = Pattern.matches(decimalPattern, value);

              if (isdouble) {
                try {
                  values.put(field, Double.parseDouble(value));
                } catch (Exception e) {
                  values.put(field, value);
                }
              } else {
                try {
                  values.put(field, Long.parseLong(value));
                } catch (Exception e) {
                  values.put(field, value);
                }
              }
            }
          }
          logger.info(values.toString());

          rs.close();
          stmt.close();

          if(PREFIX != null && !PREFIX.trim().isEmpty()) {
            values = appendPrefixToDevicenames(values, PREFIX);
          }
          
          prever.sendData(values);

        } catch (Throwable e) {
          logger.error("Mysql/Mariadb monitor has an error", e);
        } finally {
          time = rtime;
          try {
            if (stmt != null && !stmt.isClosed())
              stmt.close();
          } catch (SQLException se2) {
          }
        }
      }
    }, 0, period);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
          try {
            if (stmt != null && !stmt.isClosed()) {
              stmt.close();
            }
          } catch (SQLException e) {
          }
        if (conn != null) {
          try {
            conn.close();
          } catch (SQLException e) {
          }
        }
      }
    });
  }

  private static Map<String, Object> appendPrefixToDevicenames(Map<String, Object> values, String PREFIX) {
    Map<String, Object> prefixAppendedValues = new HashMap<String, Object>();
    for(String key : values.keySet()) {
      prefixAppendedValues.put(PREFIX + "_" + key, values.get(key));
    }
    return prefixAppendedValues;
  }

  private static void initVariables() {
    queries = -1;
    bytes_in = -1;
    bytes_out = -1;
    time = 0;
  }

  private static Map<String, String> extractFields() {
    Properties properties = PangProperties.getProperties();
    Set<Entry<Object, Object>> entrySet = properties.entrySet();
    Iterator<Entry<Object, Object>> iterator = entrySet.iterator();

    Map<String, String> targets = new HashMap<String, String>();

    while (iterator.hasNext()) {
      Entry<Object, Object> next = iterator.next();
      String key = (String) next.getKey();
      if (key.startsWith("status.")) {
        String field = key.substring(key.indexOf(".") + 1);

        String value = (String) next.getValue();
        if (value.trim().equalsIgnoreCase("true")) {
          targets.put(field.toLowerCase(), field);
        }
      }
    }

    return targets;
  }

  private static void getTrafficIn(final boolean traffic_in, long rtime,
      Map<String, Object> values, String field, String value) {
    try {
      long curBytes_in = Long.parseLong(value);
      if (bytes_in > -1) {
        values.put(TRAFFIC_IN, rountTo2decimal(((curBytes_in - bytes_in) / 1024)
            / ((double)(rtime - time) / 1000)));
      }
      bytes_in = curBytes_in;
    } catch (Exception e) {
    }
  }

  private static void getTrafficOut(long rtime,
      Map<String, Object> values, String field, String value) {
    try {
      long curBytes_out = Long.parseLong(value);
      if (bytes_out > -1) {
        values.put(TRAFFIC_OUT, rountTo2decimal(((curBytes_out - bytes_out) / 1024)
            / ((double)(rtime - time) / 1000)));
      }
      bytes_out = curBytes_out;
    } catch (Exception e) {
      logger.error("ee", e);
    }
  }

  private static void getQueriesPerSecond(final boolean queries_per_second, long rtime,
      Map<String, Object> values, String field, String value) {
    try {
      long curQueries = Long.parseLong(value);
      if (queries > -1) {

        values.put(QUERIES_PER_SECOND, rountTo2decimal((curQueries - queries)
            / ((double)(rtime - time) / 1000)));
      }
      queries = curQueries;
    } catch (Exception e) {
    }
  }

  private static void getReadsPerSecond(long rtime, Map<String, Object> values, String field,
      String value) {
    try {
      int curReads = Integer.parseInt(value);
      if (reads > -1) {
        values.put(READS_PER_SECOND, rountTo2decimal((curReads - reads)
            / ((double)(rtime - time) / 1000)));
      }
      reads = curReads;
    } catch (Exception e) {
    }
  }

  private static void getWritesPerSecond(long rtime, Map<String, Object> values, String field,
      String value) {
    try {
      int curWrites = Integer.parseInt(value);
      if (writes > -1) {
        values.put(WRITES_PER_SECOND, rountTo2decimal((curWrites - writes)
            / ((double)(rtime - time) / 1000)));
      }
      writes = curWrites;
    } catch (Exception e) {
    }
  }

  private static void getBufferUsage(String field, String value) {
    try {
      int pages = Integer.parseInt(value);
      if (field.equals("innodb_buffer_pool_pages_data")) {
        data_page = pages;
      } else if (field.equals("innodb_buffer_pool_pages_total")) {
        total_page = pages;
      }
    } catch (Exception e) {
    }
  }

  private static Connection getConn() throws Exception {
    try {
    if (conn == null || conn.isClosed()) {
      Class.forName(JDBC_DRIVER);
      conn = DriverManager.getConnection(DB_URL, USER, PASS);
      initVariables();
    }
    } catch (Exception e) {
      logger.error("Connect error occured", e);
      throw e;
    }

    return conn;
  }

  public static double rountTo2decimal(double d) {
    return Math.round(d * 100.0) / 100.0;
  }
}
