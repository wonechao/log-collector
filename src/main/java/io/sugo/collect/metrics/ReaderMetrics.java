package io.sugo.collect.metrics;

/**
 * Created by fengxj on 8/12/17.
 */

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ReaderMetrics {

  private ReaderMetrics preReaderMetrics;
  private Map<Long, AtomicLong> successMap = new ConcurrentHashMap<>();
  private AtomicLong error = new AtomicLong(0);
  public ReaderMetrics(){
    this(false);
  }
  public ReaderMetrics(boolean hasNoPre){
    if (!hasNoPre) {
      preReaderMetrics = new ReaderMetrics(true);
    }
  }

  public void incrementSuccess(String ts) {
    long timestamp = Long.parseLong(ts);
    Long key = (long) (Math.ceil((timestamp / 60000d))) * 60000;
    if (!successMap.containsKey(key)) {
      successMap.put(key, new AtomicLong(0));
    }
    successMap.get(key).incrementAndGet();
  }

  public void incrementError() {
    error.incrementAndGet();
  }

  public List<Object[]> success() {

    List<Object[]> success = new ArrayList<>();
    long current = System.currentTimeMillis();
    long oneDay = 1000 * 60 * 60 * 24;
    for (Long ts: successMap.keySet()) {
      if (current - ts > oneDay) {
        successMap.remove(ts);
        continue;
      }
      long successLong = successMap.get(ts).get();
      long preSuccessLong;
      if (!preReaderMetrics.successMap.containsKey(ts)) {
        preReaderMetrics.successMap.put(ts, new AtomicLong(0));
      }
      preSuccessLong = preReaderMetrics.successMap.get(ts).get();
      preReaderMetrics.successMap.get(ts).set(successLong);
      long difference = successLong - preSuccessLong;
      // filter invalid data
      if (difference != 0) {
        Object[] objects = new Object[2];
        objects[0] = ts;
        objects[1] = difference;
        success.add(objects);
      }
    }
    return success;
  }

  public long error() {
    long errorLong = error.get();
    long preErrorLong = preReaderMetrics.error.get();
    preReaderMetrics.error.set(errorLong);
    return errorLong - preErrorLong;
  }

  public List<Object[]> allSuccess() {

    List<Object[]> success = new ArrayList<>();
    long oneDay = 1000 * 60 * 60 * 24;
    for (Long ts: successMap.keySet()) {
      long successLong = successMap.get(ts).get();
      Object[] objects = new Object[2];
      objects[0] = ts;
      objects[1] = successLong;
      success.add(objects);
    }
    return success;
  }

  public long allError() {
    return error.get();
  }


}