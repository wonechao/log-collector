package io.sugo.collect.metrics;
import java.util.Map;
/**
 * Created by fengxj on 8/12/17.
 */
public class KairosDBMetric {
  private String name;
  private String type;
  private long timestamp;
  private Object value;
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

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

}
