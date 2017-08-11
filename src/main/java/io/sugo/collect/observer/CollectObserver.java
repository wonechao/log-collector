package io.sugo.collect.observer;

import io.sugo.collect.Configure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zack on 10/8/17.
 */
public class CollectObserver {

    private static CollectObserver instance;

    private static final Logger logger = LoggerFactory.getLogger(CollectObserver.class);
    private Observe observe;
    private ExecutorService notifyThreadPool;

    public static void init(Configure configure) {
        if (instance == null) {
            instance = new CollectObserver(configure);
        } else {
            logger.error("CollectObserver has been initialized, to use it, please call CollectObserver.shareInstance static method");
        }
    }

    public static CollectObserver shareInstance() {
        if (instance == null) {
            logger.error("CollectObserver is not initialized, please call CollectObserver.init static method first");
        }
        return instance;
    }

    private CollectObserver(Configure configure) {
        this.observe = new Observe(configure);
        this.notifyThreadPool = Executors.newFixedThreadPool(1);
    }

    public void executeObserve() {
        this.notifyThreadPool.execute(this.observe);
    }

    public void shutdownObserve() {
        this.observe.stopRunning();
        this.notifyThreadPool.shutdown();
    }
    /*  object structure
        {
            "metric_name_0": data1,
            "metric_name_1": data2
        }
     */
    public void observe(String directory, Long timestamp, HashMap<String, Object> object) {
        for (String key: object.keySet()) {
            HashMap<String, Object> data = new HashMap<>();
            data.put("name", key);
            data.put("timestamp", timestamp);
            data.put("datapoint", object.get(key));
            data.put("directory", directory);
            this.observe.addData(data);
        }
    }


}
