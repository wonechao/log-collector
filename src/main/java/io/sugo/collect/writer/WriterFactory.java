package io.sugo.collect.writer;

import io.sugo.collect.Configure;

import java.lang.reflect.Constructor;

/**
 * Created by fengxj on 4/8/17.
 */
public class WriterFactory {
  private final Configure conf;

  public WriterFactory(Configure conf) {
    this.conf = conf;
  }

  public AbstractWriter createWriter() throws Exception {
    Class onwClass = Class.forName(conf.getProperty(Configure.WRITER_CLASS));
    Constructor constructor = onwClass.getDeclaredConstructor(new Class[]{Configure.class});
    AbstractWriter writer = (AbstractWriter) constructor.newInstance(new Object[]{conf});
    return writer;
  }
}
