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
      //System.out.println(message);
    }
    return true;
  }
}
