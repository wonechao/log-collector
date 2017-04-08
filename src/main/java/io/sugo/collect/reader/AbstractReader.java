package io.sugo.collect.reader;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;

/**
 * Created by fengxj on 4/8/17.
 */
public abstract class AbstractReader {
  protected Configure conf;
  protected AbstractWriter writer;

  protected AbstractReader() {
  }

  protected AbstractReader(Configure conf, AbstractWriter writer) {
    this.conf = conf;
    this.writer = writer;
  }

  public abstract void read();
}
