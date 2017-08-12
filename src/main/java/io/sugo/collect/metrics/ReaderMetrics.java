package io.sugo.collect.metrics;

/**
 * Created by fengxj on 8/12/17.
 */

import java.util.concurrent.atomic.AtomicLong;

public class ReaderMetrics {

  private ReaderMetrics preReaderMetrics;
  private AtomicLong success = new AtomicLong(0);
  private AtomicLong error = new AtomicLong(0);
  public ReaderMetrics(){
    this(false);
  }
  public ReaderMetrics(boolean hasNoPre){
    if (!hasNoPre) {
      preReaderMetrics = new ReaderMetrics(true);
    }
  }
  public void incrementSuccess() {
    success.incrementAndGet();
  }

  public void incrementError() {
    error.incrementAndGet();
  }

  public long success() {
    long successLong = success.get();
    long preSuccessLong = preReaderMetrics.success.get();
    preReaderMetrics.success.set(successLong);
    return successLong - preSuccessLong;
  }

  public long error() {
    long errorLong = error.get();
    long preErrorLong = preReaderMetrics.error.get();
    preReaderMetrics.error.set(errorLong);
    return errorLong - preErrorLong;
  }

  public long allSuccess() {
    return success.get();
  }

  public long allError() {
    return error.get();
  }


}