package com.yahoo.ycsb.db;
// import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * ttlcount class ye.
 */
public class TTLBlob {
  // @JsonIgnoreProperties(ignoreUnknown = true)
  private String idCount;

  // public TTLBlob(String count) {
  //   this.idCount = count;
  // }
  public void setCount(String count) {
    this.idCount = count;
  }
  public String getIDCount(){
    return this.idCount;
  }
}
