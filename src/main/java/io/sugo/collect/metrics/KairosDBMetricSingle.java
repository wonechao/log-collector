package io.sugo.collect.metrics;

public class KairosDBMetricSingle extends KairosDBMetric {

    private long timestamp;
    private Object value;

    public KairosDBMetricSingle(){
        super();
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

}
