package io.sugo.collect.reader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sugo.collect.Configure;
import io.sugo.collect.metrics.KairosDBMetric;
import io.sugo.collect.metrics.ReaderMetrics;
import io.sugo.collect.parser.AbstractParser;
import io.sugo.collect.util.HttpUtil;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by fengxj on 4/8/17.
 */
public abstract class AbstractReader {
  protected Configure conf;
  protected AbstractWriter writer;
  protected AbstractParser parser;
  protected boolean running;
  protected ConcurrentHashMap<String, ReaderMetrics> readMetricMap = new ConcurrentHashMap<String, ReaderMetrics>();
  protected String host;
  protected String metricServerUrl;

  private final String METRIC_PREFIX = "collector.";
  private final String READ_LINE_METRIC_NAME = METRIC_PREFIX + "line.read.success";
  private final String READ_ERROR_METRIC_NAME = METRIC_PREFIX + "line.read.error";
  protected AbstractReader(Configure conf, AbstractWriter writer) {
    this.conf = conf;
    this.writer = writer;
    try {
      String pasterClassName = conf.getProperty(Configure.PARSER_CLASS);
      if (StringUtils.isBlank(pasterClassName))
        return;
      Class onwClass = Class.forName(pasterClassName);
      Constructor constructor = onwClass.getDeclaredConstructor(new Class[]{Configure.class});
      parser = (AbstractParser) constructor.newInstance(new Object[]{conf});
    } catch (Exception e) {
      e.printStackTrace();
    }
    this.running = true;

    metricServerUrl = conf.getProperty(Configure.METRIC_SERVER_URL);
    if (StringUtils.isNotBlank(metricServerUrl)){
      new MetricSender().start();
    }
  }


  private class MetricSender extends Thread {
    private final Gson gson = new GsonBuilder().create();
    @Override
    public void run() {
      int interval = conf.getInt(Configure.METRIC_SEND_INTERVAL, 60000);
      while (running){
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        Set<String> keySet = readMetricMap.keySet();
        long current = System.currentTimeMillis();
        List<KairosDBMetric> metrics = new ArrayList<>();
        KairosDBMetric kairosDBMetric;
        for (String key : keySet) {
          ReaderMetrics readerMetric = readMetricMap.get(key);
          Map<String, String> tags = new HashMap<String, String>();
          tags.put("from", key);
          tags.put("host", host);
          kairosDBMetric = new KairosDBMetric();
          kairosDBMetric.setName(READ_LINE_METRIC_NAME);
          kairosDBMetric.setTags(tags);
          kairosDBMetric.setType("long");
          kairosDBMetric.setValue(readerMetric.success());
          kairosDBMetric.setTimestamp(current);
          metrics.add(kairosDBMetric);

          kairosDBMetric = new KairosDBMetric();
          kairosDBMetric.setName(READ_ERROR_METRIC_NAME);
          kairosDBMetric.setTags(tags);
          kairosDBMetric.setType("long");
          kairosDBMetric.setValue(readerMetric.error());
          kairosDBMetric.setTimestamp(current);
          metrics.add(kairosDBMetric);
        }

        if (metrics.size() > 0)
        {
          HttpUtil.post(metricServerUrl,gson.toJson(metrics));
        }
      }
    }
  }

  public void stop() {
    this.running = false;
  }

  public abstract void read();
}
