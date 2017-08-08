package io.sugo.collect.writer.gateway;

import io.sugo.collect.Configure;
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
  private static final String KAFKA_TOPIC = "writer.kafka.topic";
  private static final String GATEWAY_HOST = "gateway.host";
  private final String host;
  private final String topic;

  public GateWayWriter(Configure conf) {
    super(conf);
    this.host = conf.getProperty(GATEWAY_HOST);
    this.topic = conf.getProperty(KAFKA_TOPIC);
  }

  @Override
  public boolean write(List<String> messages) {

    Gson gson = new Gson();
    String messagesJSON = gson.toJson(messages);

    return flush(messagesJSON);
  }

  private boolean flush(String jsonString) {
    boolean isSucceeded = false;

    HttpClient client = new HttpClient();
    PostMethod method = new PostMethod(this.host + "/post");
    try {
      NameValuePair[] queries = {
              new NameValuePair("locate", this.topic)
      };
      method.setQueryString(queries);
      method.addRequestHeader("Accept-Encoding","gzip");
      method.addRequestHeader("Content-Type", "application/json");
      StringRequestEntity requestEntity = new StringRequestEntity(
              jsonString,
              "application/json",
              "UTF-8"
      );
      method.setRequestEntity(requestEntity);
      int statusCode = client.executeMethod(method);
      byte[] responseBody = method.getResponseBody();
      logger.debug(new String(responseBody));
      if (statusCode != HttpStatus.SC_OK) {
        logger.warn("Method failed: " + method.getStatusLine());
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("success to send to gateway");
        }
        isSucceeded = true;
      }
    } catch (HttpException e) {
      logger.error("Fatal protocol violation: " + e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      logger.error("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
    } finally {
      method.releaseConnection();
    }

    return isSucceeded;
  }

}
