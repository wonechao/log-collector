package io.sugo.collect.reader;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;

import java.lang.reflect.Constructor;

/**
 * Created by fengxj on 4/8/17.
 */
public class ReaderFactory {
  private final Configure conf;

  public ReaderFactory(Configure conf) {
    this.conf = conf;
  }

  public AbstractReader createWriter(AbstractWriter writer) throws Exception {
    Class onwClass = Class.forName(conf.getProperty(Configure.READER_CLASS));
    Constructor constructor = onwClass.getDeclaredConstructor(new Class[]{Configure.class, AbstractWriter.class});
    AbstractReader reader = (AbstractReader) constructor.newInstance(new Object[]{conf, writer});
    return reader;
  }
}
