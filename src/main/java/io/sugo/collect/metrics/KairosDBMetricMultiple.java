package io.sugo.collect.metrics;

import java.util.List;

public class KairosDBMetricMultiple extends KairosDBMetric {

    private List<Object[]> datapoints;

    public KairosDBMetricMultiple() {
        super();
    }

    public List<Object[]> getDatapoints() {
        return this.datapoints;
    }

    public void setDatapoints(List<Object[]> datapoints) {
        this.datapoints = datapoints;
    }


}
