package io.sugo.collect.util;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import java.io.IOException;

/**
 * Created by fengxj on 7/20/17.
 */
public class HttpUtil {
  public static String post(String url,String data) throws IOException{
    HttpClient client = new HttpClient();
    PostMethod postMethod = new PostMethod(url);
    try {
      postMethod.setRequestEntity(new StringRequestEntity(data, "text/plain", "UTF-8"));
      int statusCode = client.executeMethod(postMethod);

      if (!(statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_NO_CONTENT)) {
        throw new IOException("Method failed: " + postMethod.getStatusLine());
      }

      byte[] bytes = postMethod.getResponseBody();
      if (bytes == null)
        return "";
      return new String(bytes);
    } finally {
      // Release the connection.
      postMethod.releaseConnection();
    }
  }
}
