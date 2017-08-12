/**
 * Created by zack on 12/8/17.
 */
package io.sugo.collect.observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsData {

    private static final Logger logger = LoggerFactory.getLogger(CollectObserver.class);
    private AtomicLong lines = new AtomicLong(0);
    private AtomicLong error = new AtomicLong(0);
    private AtomicLong _lines = new AtomicLong(lines.longValue());
    private AtomicLong _error = new AtomicLong(error.longValue());

    public void increaseLong(String property) {
        try {
            Field var = this.getClass().getDeclaredField(property);
            Long value = ((AtomicLong)var.get(this)).longValue();
            var.set(this, new AtomicLong(value + 1));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            logger.error("increase failed: ", e);
        }
    }

    public Long countLong(String property) {
        Long result = 0L;
        try {
            Field var = this.getClass().getDeclaredField(property);
            Long value = ((AtomicLong)var.get(this)).longValue();
            Field _var = this.getClass().getDeclaredField("_" + property);
            Long _value = ((AtomicLong)_var.get(this)).longValue();
            result = value - _value;
            _var.set(this, new AtomicLong(value));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            logger.error("increase failed: ", e);
        }
        return result;
    }



}
