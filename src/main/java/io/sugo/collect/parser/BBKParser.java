package io.sugo.collect.parser;

import io.sugo.collect.Configure;

import java.util.Map;

/**
 * Created by sugo on 17/6/13.
 */
public class BBKParser extends GrokParser{

  private static final String EXCEPTION_KEY = "exception";
  public BBKParser(Configure conf) {
    super(conf);
  }

  @Override
  public Map<String, Object> parse(String line) {
    Map<String, Object> map = super.parse(line);
    if (map.containsKey(EXCEPTION_KEY)){
      map.put("t|" + EXCEPTION_KEY, map.get(EXCEPTION_KEY));
      map.remove(EXCEPTION_KEY);
    }
    return map;
  }
}
