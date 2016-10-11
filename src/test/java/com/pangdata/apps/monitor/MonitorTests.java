package com.pangdata.apps.monitor;

import org.junit.Test;

public class MonitorTests {
  
  @Test
  public void test1() {
    String value = "1000";
    long prevQueries = 0;
    long time = 1000;
    long rtime = 1900;
    
    long curQueries = Long.parseLong(value);
    
    double rountTo2decimal1 = MysqlMariaDBMonitor.rountTo2decimal((curQueries - prevQueries)
        / ((double)(rtime - time) / 1000));
    
    double rountTo2decimal2 = MysqlMariaDBMonitor.rountTo2decimal((curQueries - prevQueries)
        / (double)((rtime - time) / 1000));
    
    System.out.println("before : "+rountTo2decimal1);
    System.out.println("after : "+rountTo2decimal2);
  }
  
}
