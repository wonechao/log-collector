package io.sugo.collect.metrics;
import java.util.List;
import java.util.Map;
/**
 * Created by fengxj on 8/12/17.
 */
public abstract class KairosDBMetric {
  private String name;
  private String type;
  private Map<String, String> tags;

  public KairosDBMetric(){
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

}
