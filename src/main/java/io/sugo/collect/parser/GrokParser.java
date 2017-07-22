package io.sugo.collect.parser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sugo.collect.Configure;
import io.sugo.collect.util.IpConverter;
import io.sugo.grok.api.Grok;
import io.sugo.grok.api.Match;
import io.sugo.grok.api.exception.GrokException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by fengxj on 6/10/17.
 */
public class GrokParser extends AbstractParser {
  private final Logger logger = LoggerFactory.getLogger(GrokParser.class);
  private static final Gson gson = new GsonBuilder().create();
  public static final String FILE_READER_GROK_PATTERNS_PATH = "file.reader.grok.patterns.path";
  public static final String FILE_READER_GROK_EXPR = "file.reader.grok.expr";
  public static final String FILE_READER_GROK_IP_FILE = "file.reader.grok.ip.file";
  public static final String FILE_READER_GROK_IP_FIELD = "file.reader.grok.ip.field";
  public static final String FILE_READER_GROK_IP_NEEDFIELD = "file.reader.grok.ip.needField";


  private IpConverter ipConverter;
  private Grok grok;
  private String ipField;

  public GrokParser(Configure conf) {
    super(conf);
    try {
      String patternPath = conf.getProperty(FILE_READER_GROK_PATTERNS_PATH);
      if (StringUtils.isBlank(patternPath)){
        patternPath = conf.getProperty(Configure.USER_DIR) + "conf/patterns";
      }
      if (!patternPath.startsWith("/"))
        patternPath = conf.getProperty(Configure.USER_DIR) + "/" + patternPath;
      logger.info("final patternPath:" + patternPath);
      grok = Grok.create(patternPath);
      String grokExpr = conf.getProperty(FILE_READER_GROK_EXPR);
      if (StringUtils.isBlank(grokExpr)){
        logger.error(FILE_READER_GROK_EXPR + "must be set!");
        System.exit(1);
      }
      grok.compile(grokExpr);

      ipField = conf.getProperty(FILE_READER_GROK_IP_FIELD);
      if (StringUtils.isNotBlank(ipField)){
        String ipFile = conf.getProperty(FILE_READER_GROK_IP_FILE);
        if (StringUtils.isBlank(ipFile)){
          ipFile = conf.getProperty(Configure.USER_DIR) + "/conf/sugoip.txt";
        } else {
          if (!ipFile.startsWith("/"))
            ipFile = conf.getProperty(Configure.USER_DIR) + "/" + ipFile;
        }

        logger.info("loading ip library...");
        String needFieldStr = conf.getProperty(FILE_READER_GROK_IP_NEEDFIELD);
        if (StringUtils.isNotBlank(needFieldStr)){
          String[] needFields = needFieldStr.split(",");
          Set<String> fieldSet = new HashSet<>(Arrays.asList(needFields));
          ipConverter = new IpConverter(ipFile, fieldSet);
        } else {
          ipConverter = new IpConverter(ipFile);
        }

        logger.info("ip library loaded finish");
      }

    } catch (GrokException e) {
      logger.error("", e);
      System.exit(1);
    }
  }

  @Override
  public Map<String, Object> parse(String line) throws Exception {
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
        jsonMap.put(key, gson.fromJson(jsonStr, Map.class));

      }
    }
    if(jsonMap == null)
      return gmMap;

    //ip解析
    if (StringUtils.isNotBlank(ipField)){
       String ip = (String) gmMap.get(ipField);
       Map<String,Object> ipDetailMap = ipConverter.getIpDetail(ip);
       gmMap.putAll(ipDetailMap);

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
