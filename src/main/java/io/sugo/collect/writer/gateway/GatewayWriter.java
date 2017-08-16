package io.sugo.collect.writer.gateway;

import io.sugo.collect.Configure;
import io.sugo.collect.util.HttpUtil;
import io.sugo.collect.writer.AbstractWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


/**
 * Created by fengxj on 5/24/17.
 */
public class GatewayWriter extends AbstractWriter {

  private final Logger logger = LoggerFactory.getLogger(GatewayWriter.class);
  private static final String WRITER_GATEWAY_API = "writer.gateway.api";
  private final String api;

  public GatewayWriter(Configure conf) {
    super(conf);
    this.api = conf.getProperty(WRITER_GATEWAY_API);
  }

  @Override
  public boolean write(List<String> messages) {

    StringBuilder result = new StringBuilder();
    for (String message : messages) {
      result.append(message).append("\n");
    }
    return flush(result.toString());
  }

  private boolean flush(String jsonString) {
    try {
      HttpUtil.post(this.api, jsonString);
      return true;
    } catch (IOException e) {
      logger.error("", e);
      return false;
    }
  }

}
