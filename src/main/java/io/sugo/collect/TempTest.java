package io.sugo.collect;

import com.google.gson.Gson;
import io.sugo.grok.api.Grok;
import io.sugo.grok.api.Match;
import io.sugo.grok.api.exception.GrokException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by sugo on 17/6/8.
 */
public class TempTest {
  public static void main(String[] args) throws GrokException, ParseException {

    String tmpstr = "[2017-06-14T19:06:34+08:00] 117.136.41.12 \"GET /watchaccount/accountAPP/list/59cf31a348e84591a9355cc0f4cb278727274229?uuid=501ffa2b2bab4908b9253e7f4ce6b68d HTTP/1.1\" 200 0.026 0.026 192.168.12.236:7081 ygee7OZedaATHnVDl2xqDbXIibmRaXtQCfiFhVx5eQAyWd2Ysy4BYRq9R6PQYvuCoDZcTEJkZRai/axMFxoxjrbzMop0RoWgCobs8sA0SklQmF2bu0xw5lzf+Gy3IsAjjeEGtj/FCWZjjAKAH2wJSV2AEtkzqKstbnfgs+DUeQuWE3SVMjDKn+8m5nWbwPntKZ2EdBlGunDeQMg37MngqAb2ls7OWkGCG27Kzaq7mUnwSP/DW8rXj/2IHq/L1vRw+/F4/W0zX2F0Mz/TGrctf72jUp/CnkcB39fJNJwJhfdxpnKrAwTCsAwa3loQ11c1+oTC+EkQ1o7iFNIU/9kE7Q== okhttp/3.4.1 tR8WHRAByc2iVtikGM/SDpvifHpeFuD9jgmptyiGSjS8fZHbfAJymg8V7Bc4+rFE6u6kIKwERjAq8bL5JAZXLQ==";
    Grok grok = Grok.create("/Users/sugo/log-collector/src/main/resources/patterns");

/** Grok pattern to compile, here httpd logs */
    grok.compile("\\[%{CUSTOM_TIMESTAMP_ISO8601:logtime;date;yyyy-MM-dd'T'HH:mm:ssXXX}\\] %{IPV4:remote_addr} \"(?:%{WORD:request_method} %{NOTSPACE:request_url}(?: HTTP/%{NUMBER:httpversion})?|(-))\" %{NOTSPACE:status} %{NOTSPACE:request_time:float} %{NOTSPACE:upstream_response_time:float} %{NOTSPACE:upstream_addr} %{NOTSPACE:json_base_request} %{CUSTOM_DATA:agent} %{NOTSPACE:s_key}");
    /** Line of log to match */
    Match gm = grok.match(tmpstr);
    gm.captures();
    Map<String,Object> map= gm.toMap();
    Gson gson = new Gson();
    System.out.println(gson.toJson(map));
    System.out.println(map.get("s_key"));
/** Get the output */
    //System.out.println(gm.toJson());
  }
}
