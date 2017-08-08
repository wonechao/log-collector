package io.sugo.collect.writer.console;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;

import java.util.List;

/**
 * Created by fengxj on 5/17/17.
 */
public class ConsoleWriter extends AbstractWriter {

  public ConsoleWriter(Configure conf) {
    super(conf);
  }

  @Override
  public boolean write(List<String> messages) {
    for (String message : messages) {
      /*
      message structure:
      {
        "filename":"*.*",
        "host":"*.*.*.*",
        "key1":"value1",
        "key2":"value2",
        "key3":"value3","
        directory":"/"
      }
       */
      System.out.println(message);
    }
    return true;
  }
}
