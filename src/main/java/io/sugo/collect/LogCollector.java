package io.sugo.collect;

import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.reader.ReaderFactory;
import io.sugo.collect.writer.AbstractWriter;
import io.sugo.collect.writer.WriterFactory;

import java.lang.reflect.Constructor;

/**
 * Created by fengxj on 4/8/17.
 */
public class LogCollector {
  public static void main(String[] args) throws Exception {
    Configure conf = new Configure();
    AbstractWriter writer = new WriterFactory(conf).createWriter();
    AbstractReader reader = new ReaderFactory(conf).createWriter(writer);
    reader.read();
  }
}
