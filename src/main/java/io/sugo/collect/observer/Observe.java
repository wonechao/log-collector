/**
 * Created by zack on 10/8/17.
 */
package io.sugo.collect.observer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.sugo.collect.Configure;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Observe implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Observe.class);
    private static long notifyInterval = 30 * 1000;
    private static final String OBSERVER_API = "observer.api";
    private static final String FILE_READER_HOST = "file.reader.host";
    public static final String COLLECTED_LINES = "sugo_collected_lines";
    public static final String COLLECTED_ERROR = "sugo_collected_error";

    private boolean isRunning;
    private String api;
    private String host;
    private String appid;
    /*  queue structure
        [
            {
                "name": "",
                "timestamp": "",
                "datapoint": ""
            },{
                "name": "",
                "timestamp": "",
                "datapoint": ""
            }
        ]
     */
    private List<HashMap<String, Object>> queue;
    ReadWriteLock rwlock;

    Observe(Configure configure) {

        this.isRunning = true;
        this.api = configure.getProperty(OBSERVER_API);
        this.host = configure.getProperty(FILE_READER_HOST);
        this.appid = "sugo_collect";
        this.queue = new ArrayList<>();
        this.rwlock = new ReentrantReadWriteLock();
    }

    @Override
    public void run() {

        if (this.api.isEmpty()) {
            return;
        }

        while (this.isRunning) {

            rwlock.readLock().lock();
            int queueSize = this.queue.size();
            rwlock.readLock().unlock();
            if (queueSize > 0) {
                packToFlush();
            }

            try {
                Thread.sleep(Observe.notifyInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        rwlock.readLock().lock();
        int queueSize = this.queue.size();
        rwlock.readLock().unlock();
        if (queueSize > 0) {
            // 当被shutdown的时候，如果队列中还有数据，则立即发送
            packToFlush();
        }

    }

    void stopRunning() {
        this.isRunning = false;
    }

    /*  data structure
        {
            "name": "",
            "timestamp": "",
            "datapoint": "",
            "directory": ""
        }
     */
    void addData(HashMap<String, Object> data) {
        rwlock.writeLock().lock();
        this.queue.add(data);
        rwlock.writeLock().unlock();
    }

    private void packToFlush() {

        rwlock.readLock().lock();
        List<HashMap<String, Object>> flushQueue = new ArrayList<>();
        flushQueue.addAll(this.queue);
        rwlock.readLock().unlock();

        String json = pack(flushQueue);
        if (flush(json)) {
            rwlock.writeLock().lock();
            this.queue.removeAll(flushQueue);
            rwlock.writeLock().unlock();
        }
    }

    private String pack(List<HashMap<String, Object>> flushQueue) {
        List<HashMap<String, Object>> packageQueue = new ArrayList<>();
        HashMap<String, Object> metricLines = new HashMap<>();
        HashMap<String, Object> metricError = new HashMap<>();
        List<Object> linesDataPoints = new ArrayList<>();
        List<Object> errorDataPoints = new ArrayList<>();
        HashMap<String, Object> tags = new HashMap<>();
        String jsonString = "";

        metricLines.put("name", COLLECTED_LINES);
        metricError.put("name", COLLECTED_ERROR);

        for (HashMap<String, Object> data : flushQueue) {
            String dataName = (String)data.get("name");
            switch (dataName) {
                case COLLECTED_LINES:
                    List<Object> lineDataPoint = new ArrayList<>();
                    lineDataPoint.add(data.get("timestamp"));
                    lineDataPoint.add(data.get("datapoint"));
                    linesDataPoints.add(lineDataPoint);
                    metricLines.put("datapoints", linesDataPoints);
                    break;
                case COLLECTED_ERROR:
                    List<Object> errorDataPoint = new ArrayList<>();
                    errorDataPoint.add(data.get("timestamp"));
                    errorDataPoint.add(data.get("datapoint"));
                    errorDataPoints.add(errorDataPoint);
                    metricError.put("datapoints", errorDataPoints);
                    break;
                default:
                    if (logger.isDebugEnabled()) {
                        logger.debug("wrong name: ", dataName);
                    }
                    break;
            }
            tags.put("directory", data.get("directory"));
        }
        tags.put("host", this.host);
        tags.put("appid", this.appid);
        metricLines.put("tags", tags);
        metricError.put("tags", tags);
        packageQueue.add(metricLines);
        packageQueue.add(metricError);

        Gson gson = new Gson();
        jsonString = gson.toJson(packageQueue);
        return jsonString;
    }

    private boolean flush(String jsonString) {
        boolean isSucceeded = false;
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
                isSucceeded = true;
            }
        } catch (HttpException e) {
            logger.error("Fatal protocol violation: ", e);
        } catch (IOException e) {
            logger.error("Fatal transport error: ", e);
        } finally {
            method.releaseConnection();
        }
        return isSucceeded;
    }

}
