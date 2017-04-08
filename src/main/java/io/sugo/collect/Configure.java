package io.sugo.collect;

import org.apache.commons.lang.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by fengxj on 3/29/17.
 */
public class Configure {
  private static final String CLASSPATH_URL_PREFIX = "classpath:";
  private static final String KAFKA_PROPERTIES = "kafka.properties";
  private static final String COLLECT_PROPERTIES = "collect.properties";
  public static final String WRITER_CLASS = "writer.class";
  private String collectorConf;
  private String kafkaConf;
  private Properties properties = new Properties();
  private Properties kafkaProperties = new Properties();

  public Configure() {
    collectorConf = System.getProperty(COLLECT_PROPERTIES, CLASSPATH_URL_PREFIX + COLLECT_PROPERTIES);
    kafkaConf = System.getProperty(KAFKA_PROPERTIES, CLASSPATH_URL_PREFIX + KAFKA_PROPERTIES);
    loadConf();
  }

  private void loadConf() {
    try {
      if (collectorConf.startsWith(CLASSPATH_URL_PREFIX)) {
        collectorConf = StringUtils.substringAfter(collectorConf, CLASSPATH_URL_PREFIX);
        properties.load(Configure.class.getClassLoader().getResourceAsStream(collectorConf));
      } else {
        properties.load(new FileInputStream(collectorConf));
      }

      for (Object key : properties.keySet()) {
        System.out.println(key + " : " + properties.getProperty(key.toString()));
      }

      if (kafkaConf.startsWith(CLASSPATH_URL_PREFIX)) {
        kafkaConf = StringUtils.substringAfter(kafkaConf, CLASSPATH_URL_PREFIX);
        kafkaProperties.load(Configure.class.getClassLoader().getResourceAsStream(kafkaConf));
      } else {
        kafkaProperties.load(new FileInputStream(kafkaConf));
      }
      for (Object key : kafkaProperties.keySet()) {
        System.out.println(key + " : " + kafkaProperties.getProperty(key.toString()));
      }

    } catch (IOException ix) {
      System.out.println(ix);
    }
  }

  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  public Properties getProperties() {
    return this.properties;
  }

  public Properties getKafkaProperties() {
    return this.kafkaProperties;
  }

  public int getInt(String key) {
    return Integer.parseInt(this.properties.get(key).toString());
  }
}
