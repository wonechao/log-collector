package io.sugo.collect.writer.kafka;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;


/**
 * Created by fengxj on 4/8/17.
 */
public class KafkaWriter extends AbstractWriter {
  public KafkaWriter(Configure conf) {
    super(conf);
  }

  @Override
  public boolean write(String message) {
    System.out.println(message);
    return true;
  }

}
