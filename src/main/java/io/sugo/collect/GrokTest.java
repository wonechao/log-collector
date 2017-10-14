package io.sugo.collect;

import io.sugo.grok.api.Grok;
import io.sugo.grok.api.Match;

import java.net.InetAddress;
import java.util.Map;

/**
 * Created by fengxj on 8/17/17.
 */
public class GrokTest {
  public static void main(String [] args) throws Exception {
//    Grok grok = Grok.create("src/main/resources/patterns");
//    grok.compile("\\[%{CUSTOM_TIMESTAMP_ISO8601:logtime;date;yyyy-MM-dd'T'HH:mm:ssXXX}\\] %{IPV4:remote_addr} \"(?:%{WORD:request_method} %{URIPATH:request_url}(?:%{URIPARAM:request_param})?(?: HTTP/%{NUMBER:httpversion})?|(-))\" %{NOTSPACE:status} %{NOTSPACE:request_time:float} %{NUMBER:request_length:int} %{BASE16NUM:bytes_sent:int} %{NOTSPACE:http_referer:string} ((%{BASE16FLOAT:upstream_response_time:float}[, ]{0,2})+|(-)) (%{UPSTREAM_ADDR:upstream_addr}|(-)) %{DATA:http_Base_Request_Param:string} \\\"%{GREEDYDATA:agent}\\\" %{DATA:http_Grey:string} %{DATA:http_serverGrey:string} %{DATA:http_eebbk_sign} %{DATA:http_eebbk_key:string} %{DATA:http_encrypted:string} %{NOTSPACE:version:string}");
//    String line = "[2017-08-30T11:18:18+08:00] 117.136.79.132 \"GET /agps/getAGPSData?xtcstamp=1504063094000V2.32 HTTP/1.1\" 200 0.002 508 2700 - 0.002 10.9.102.226:8081 {\\x22appId\\x22:\\x222\\x22,\\x22mac\\x22:\\x22eca9fa9ca846\\x22,\\x22deviceId\\x22:\\x22zsxzfdnefidbofep\\x22,\\x22token\\x22:\\x227F4674326EB1766ED73A8661EB1203FB\\x22,\\x22timestamp\\x22:\\x222017-08-30 11:18:14\\x22,\\x22imFlag\\x22:\\x221\\x22,\\x22registId\\x22:25352923} \"-\" - - 807241D104F272B46CC62BDDCA81689A dKPLxMlRPwMUDu9MeV7vQaBhMLEovVY0UFE9Q2BD0ankF3FNo6doW2N+6Pe/jqto0gACyEl+PLrWYhNjsPKByg== - W_2.32";
//    Match gm = grok.match(line);
//    gm.captures();
//    Map<String, Object> gmMap = gm.toMap();
    System.out.println("===");
    InetAddress addr = InetAddress.getLocalHost();
    System.out.println(addr.getHostAddress());
  }
}
