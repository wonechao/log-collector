package io.sugo.collect.parser;

import io.sugo.collect.Configure;

import java.util.Map;

/**
 * Created by fengxj on 6/10/17.
 */
public abstract class AbstractParser {
  protected Configure conf;

  public AbstractParser(Configure conf){
    this.conf = conf;
  }
  public abstract Map<String, Object> parse(String line) throws Exception;
}
