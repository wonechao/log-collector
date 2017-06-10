package io.sugo.collect.parser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sugo.collect.Configure;
import io.sugo.grok.api.Grok;
import io.sugo.grok.api.Match;
import io.sugo.grok.api.exception.GrokException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by fengxj on 6/10/17.
 */
public class GrokParser extends AbstractParser {
  private final Logger logger = LoggerFactory.getLogger(GrokParser.class);
  private static final Gson gson = new GsonBuilder().create();
  public static final String FILE_READER_GROK_PATTERNS_PATH = "file.reader.grok.patterns.path";
  public static final String FILE_READER_GROK_EXPR = "file.reader.grok.expr";

  private Grok grok;

  public GrokParser(Configure conf) {
    super(conf);
    try {
      String patternPath = conf.getProperty(FILE_READER_GROK_PATTERNS_PATH);
      if (!patternPath.startsWith("/"))
        patternPath = System.getProperty("user.dir") + "/" + patternPath;
      logger.info("final patternPath:" + patternPath);
      grok = Grok.create(patternPath);
      String grokExpr = conf.getProperty(FILE_READER_GROK_EXPR);
      grok.compile(grokExpr);
    } catch (GrokException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Map<String, Object> parse(String line) {
    Match gm = grok.match(line);
    gm.captures();
    Map<String, Object> gmMap = gm.toMap();
    Map<String,Map<String, Object>> jsonMap = null;
    for (String key: gmMap.keySet()) {
      Object value = gmMap.get(key);
      if (key.startsWith("json_") && value != null){
        if (jsonMap == null)
          jsonMap = new HashMap<>();
        //参考nginx日志模块ngx_http_log_escape方法
        //" \ del  会被转为\x22 \x5C \x7F
        //https://github.com/nginx/nginx/blob/9ad18e43ac2c9956399018cbb998337943988333/src/http/modules/ngx_http_log_module.c
        String jsonStr = value.toString().replace("\\x5C","\\").replace("\\x22","\"");
        if (jsonStr.equals("-")){
          jsonMap.put(key, null);
          continue;
        }
        try {
          jsonMap.put(key, gson.fromJson(jsonStr, Map.class));
        }catch (Exception e){
          logger.error("json parse fail: " + value);
        }
        continue;
      }
    }
    for (String key: jsonMap.keySet()) {
      gmMap.remove(key);
      Map<String,Object> value = jsonMap.get(key);
      if (value != null)
        gmMap.putAll(jsonMap.get(key));
    }
    return gmMap;
  }
}
