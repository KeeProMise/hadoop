package org.apache.hadoop.hdfs.server.federation.router.async;

public abstract class AsyncProcess {
  public abstract  <T> T req();

  public abstract  <T, R> R rsp(T result);

  public <T> void process() {
    T result = req();
    rsp(result);
  }
}
