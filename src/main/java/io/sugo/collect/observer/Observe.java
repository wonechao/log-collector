/**
 * Created by zack on 10/8/17.
 */
package io.sugo.collect.observer;

import com.google.gson.Gson;
import io.sugo.collect.Configure;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class Observe implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Observe.class);
    private static long notifyInterval = 5 * 1000;
    private static final String OBSERVER_API = "observer.api";
    private static final String FILE_READER_HOST = "file.reader.host";
    public static final String COLLECTED_LINES = "sugo_collected_lines";
    public static final String COLLECTED_ERROR = "sugo_collected_error";

    private boolean isRunning;
    private String api;
    private String host;
    private String appid;
    /*  metricsData structure
        {
            "directory0": metricsData0,
            "directory1": metricsData1,
        }
     */
    private HashMap<String, MetricsData> metricsData;
    private ReadWriteLock rwlock;

    Observe(Configure configure) {

        this.isRunning = true;
        this.api = configure.getProperty(OBSERVER_API);
        this.host = configure.getProperty(FILE_READER_HOST);
        this.appid = "sugo_collect";
        this.metricsData = new HashMap<>();
        this.rwlock = new ReentrantReadWriteLock();
    }

    @Override
    public void run() {

        if (this.api.isEmpty()) {
            return;
        }
        while (this.isRunning) {
            this.rwlock.readLock().lock();
            boolean isMetricsDataEmpty = this.metricsData.isEmpty();
            this.rwlock.readLock().unlock();
            if (!isMetricsDataEmpty) {
                packToFlush();
            }
            try {
                Thread.sleep(Observe.notifyInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // 当被shutdown的时候，发送最后一次数据
        this.rwlock.readLock().lock();
        boolean isMetricsDataEmpty = this.metricsData.isEmpty();
        this.rwlock.readLock().unlock();
        if (!isMetricsDataEmpty) {
            packToFlush();
        }
    }

    void stopRunning() {
        this.isRunning = false;
    }

    void addData(String directory, HashMap<String, Object> object) {

        if (!this.metricsData.keySet().contains(directory)) {
            MetricsData md = new MetricsData();
            this.rwlock.writeLock().lock();
            this.metricsData.put(directory, md);
            this.rwlock.writeLock().unlock();
        }
        if (object.get("lines") != null && (boolean) object.get("lines")) {
            this.metricsData.get(directory).increaseLong("lines");
        }
        if (object.get("error") != null && (boolean) object.get("error")) {
            this.metricsData.get(directory).increaseLong("error");
        }
    }

    private void packToFlush() {

        Gson gson = new Gson();
        List<HashMap<String, Object>> packageQueue = packData();
        String json = gson.toJson(packageQueue);
        flush(json);
    }

    private List<HashMap<String, Object>> packData() {

        List<HashMap<String, Object>> packageQueue = new ArrayList<>();
        Long now = System.currentTimeMillis();
        for (String directory: this.metricsData.keySet()) {
            HashMap<String, Object> tags = new HashMap<>();
            tags.put("host", this.host);
            tags.put("appid", this.appid);
            tags.put("directory", directory);

            // lines
            Long linesCount = this.metricsData.get(directory).countLong("lines");
            HashMap<String, Object> linesMetric = packMetric(COLLECTED_LINES, now, linesCount, tags);
            packageQueue.add(linesMetric);

            // error
            Long errorCount = this.metricsData.get(directory).countLong("error");
            HashMap<String, Object> errorMetric = packMetric(COLLECTED_LINES, now, errorCount, tags);
            packageQueue.add(errorMetric);
        }
        return packageQueue;
    }

    private HashMap<String, Object> packMetric(String name, Long timestamp, Object data, HashMap<String, Object> tags) {
        List<List<Object>> datapoints = new ArrayList<>();
        List<Object> datapoint = new ArrayList<>();
        datapoint.add(timestamp);
        datapoint.add(data);
        datapoints.add(datapoint);
        Metric metric = new Metric(name, tags, datapoints);
        return metric.toHashMap();
    }

    private void flush(String jsonString) {
//        boolean isSucceeded = false;
        HttpClient client = new HttpClient();
        PostMethod method = new PostMethod(this.api);
        try {
            StringRequestEntity requestEntity = new StringRequestEntity(
                    jsonString,
                    "application/json",
                    "UTF-8"
            );
            method.setRequestEntity(requestEntity);
            int statusCode = client.executeMethod(method);
            if (logger.isDebugEnabled()) {
                byte[] responseBody = method.getResponseBody();
                logger.debug(new String(responseBody));
            }
            if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_NO_CONTENT) {
                logger.warn("Method failed: " + method.getStatusLine());
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("success to send to KairosDB");
                }
//                isSucceeded = true;
            }
        } catch (HttpException e) {
            logger.error("Fatal protocol violation: ", e);
        } catch (IOException e) {
            logger.error("Fatal transport error: ", e);
        } finally {
            method.releaseConnection();
        }
//        return isSucceeded;
    }

}
