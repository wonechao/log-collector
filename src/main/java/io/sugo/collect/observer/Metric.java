/**
 * Created by zack on 12/8/17.
 */
package io.sugo.collect.observer;

import java.util.HashMap;
import java.util.List;

public class Metric {

    private String name;
    private HashMap<String, Object> tags;
    private List<List<Object>> datapoints;

    public Metric(String name, HashMap<String, Object> tags, List<List<Object>> datapoints) {
        this.name = name;
        this.tags = tags;
        this.datapoints = datapoints;
    }

    public HashMap<String, Object> toHashMap() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("name", this.name);
        result.put("tags", this.tags);
        result.put("datapoints", this.datapoints);
        return result;
    }

}
