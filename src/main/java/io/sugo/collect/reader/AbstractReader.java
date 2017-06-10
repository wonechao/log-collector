package io.sugo.collect.reader;

import io.sugo.collect.Configure;
import io.sugo.collect.parser.AbstractParser;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;

/**
 * Created by fengxj on 4/8/17.
 */
public abstract class AbstractReader {
  protected Configure conf;
  protected AbstractWriter writer;
  protected AbstractParser parser;

  protected AbstractReader(Configure conf, AbstractWriter writer) {
    this.conf = conf;
    this.writer = writer;
    try {
      String pasterClassName = conf.getProperty(Configure.PARSER_CLASS);
      if (StringUtils.isBlank(pasterClassName))
        return;
      Class onwClass = Class.forName(pasterClassName);
      Constructor constructor = onwClass.getDeclaredConstructor(new Class[]{Configure.class});
      parser = (AbstractParser) constructor.newInstance(new Object[]{conf});
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public abstract void read();
}
