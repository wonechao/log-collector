package io.sugo.collect.reader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sugo.collect.Configure;
import io.sugo.collect.metrics.KairosDBMetric;
import io.sugo.collect.metrics.KairosDBMetricMultiple;
import io.sugo.collect.metrics.KairosDBMetricSingle;
import io.sugo.collect.metrics.ReaderMetrics;
import io.sugo.collect.parser.AbstractParser;
import io.sugo.collect.util.HttpUtil;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by fengxj on 4/8/17.
 */
public abstract class AbstractReader {
  private final Logger logger = LoggerFactory.getLogger(AbstractReader.class);
  protected Configure conf;
  protected AbstractWriter writer;
  protected AbstractParser parser;
  protected boolean running;
  protected ConcurrentHashMap<String, ReaderMetrics> readMetricMap = new ConcurrentHashMap<String, ReaderMetrics>();
  protected String host;
  protected String metricServerUrl;
  protected String metricDimensionTime;
  protected boolean shouldMetricSuccessProcessed;

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

    String metricSuccessStyle = conf.getProperty(Configure.METRIC_SUCCESS_STYLE, "processed");
    if (metricSuccessStyle.equals("processed")) {
      shouldMetricSuccessProcessed = true;
    } else { // raw
      shouldMetricSuccessProcessed = false;
    }
    metricDimensionTime = conf.getProperty(Configure.METRIC_DIMENSION_TIME, "_time");
    metricServerUrl = conf.getProperty(Configure.METRIC_SERVER_URL);
    if (StringUtils.isNotBlank(metricServerUrl)){
      new MetricSender().start();
    }
  }


  private class MetricSender extends Thread {
    private final Gson gson = new GsonBuilder().create();
    private List<KairosDBMetric> failMetrics = new ArrayList<KairosDBMetric>();
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
        for (String key : keySet) {
          ReaderMetrics readerMetric = readMetricMap.get(key);
          Map<String, String> tags = new HashMap<String, String>();
          tags.put("from", key);
          tags.put("host", host);
          if (shouldMetricSuccessProcessed) {
            KairosDBMetricSingle successKairosDBMetric = new KairosDBMetricSingle();
            successKairosDBMetric.setName(READ_LINE_METRIC_NAME);
            successKairosDBMetric.setTags(tags);
            successKairosDBMetric.setType("long");
            successKairosDBMetric.setValue(readerMetric.success());
            successKairosDBMetric.setTimestamp(current);
            metrics.add(successKairosDBMetric);
          } else {
            KairosDBMetricMultiple successKairosDBMetric = new KairosDBMetricMultiple();
            successKairosDBMetric.setName(READ_LINE_METRIC_NAME);
            successKairosDBMetric.setTags(tags);
            successKairosDBMetric.setType("long");
            successKairosDBMetric.setDatapoints(readerMetric.successMap());
            metrics.add(successKairosDBMetric);
          }

          KairosDBMetricSingle errorKairosDBMetric = new KairosDBMetricSingle();
          errorKairosDBMetric.setName(READ_ERROR_METRIC_NAME);
          errorKairosDBMetric.setTags(tags);
          errorKairosDBMetric.setType("long");
          errorKairosDBMetric.setValue(readerMetric.error());
          errorKairosDBMetric.setTimestamp(current);
          metrics.add(errorKairosDBMetric);
        }

        if (metrics.size() > 0)
        {
          //发送上一次失败的metric
          if (failMetrics.size() > 0) {
            try {
              HttpUtil.post(metricServerUrl,gson.toJson(failMetrics));
            } catch (IOException e) {
              logger.error("send failMetrics fail ", e);
            }
          }

          try {
            HttpUtil.post(metricServerUrl,gson.toJson(metrics));
          } catch (IOException e) {
            failMetrics.addAll(metrics);
            logger.error("send metrics fail ", e);
          }
        }
      }
    }
  }

  public void stop() {
    this.running = false;
  }

  public abstract void read();
}
