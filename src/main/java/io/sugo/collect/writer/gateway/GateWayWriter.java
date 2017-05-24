package io.sugo.collect.writer.gateway;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;

import java.util.List;

/**
 * Created by fengxj on 5/24/17.
 */
public class GateWayWriter extends AbstractWriter {

  public GateWayWriter(Configure conf) {
    super(conf);
  }

  @Override
  public boolean write(List<String> messages) {
    return false;
  }
}
