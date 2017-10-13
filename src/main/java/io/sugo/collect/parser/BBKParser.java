package io.sugo.collect.parser;

import io.sugo.collect.Configure;

import java.util.Map;

/**
 * Created by sugo on 17/6/13.
 */
public class BBKParser extends GrokParser {

  private static final String EXCEPTION_KEY = "exception";

  public BBKParser(Configure conf) {
    super(conf);
  }

  @Override
  public Map<String, Object> parse(String line) throws Exception {

    //参考nginx日志模块ngx_http_log_escape方法
    //" \ del  会被转为\x22 \x5C \x7F
    //https://github.com/nginx/nginx/blob/9ad18e43ac2c9956399018cbb998337943988333/src/http/modules/ngx_http_log_module.c
    line = line.replace("\\x5C", "\\").replace("\\x22", "\"");

    Map<String, Object> map = super.parse(line);
    if (map.containsKey(EXCEPTION_KEY)) {
      Object exVal = map.get(EXCEPTION_KEY);
      map.put("exception_size", exVal.toString().length());
    }
    return map;
  }
}
