package io.sugo.collect.writer.gateway;

import io.sugo.collect.Configure;
import io.sugo.collect.util.HttpUtil;
import io.sugo.collect.writer.AbstractWriter;

import com.google.gson.*;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PostMethod.*;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.List;


/**
 * Created by fengxj on 5/24/17.
 */
public class GateWayWriter extends AbstractWriter {

  private final Logger logger = LoggerFactory.getLogger(GateWayWriter.class);
  private static final String GATEWAY_API = "gateway.api";
  private final String api;

  public GateWayWriter(Configure conf) {
    super(conf);
    this.api = conf.getProperty(GATEWAY_API);
  }

  @Override
  public boolean write(List<String> messages) {

    StringBuilder result = new StringBuilder();
    for (String message : messages) {
      result.append(message).append("\n");
    }
    return HttpUtil.postTo(this.api, result.toString());
  }

}
