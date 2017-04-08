package io.sugo.collect.writer.kafka;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by fengxj on 4/8/17.
 */
public class KafkaWriter extends AbstractWriter {
  private Queue<String> queue = new LinkedList<String>();

  public KafkaWriter(Configure conf) {
    super(conf);
    new Producer().start();
  }

  @Override
  public void write(String message) {
    queue.add(message);
  }

  private class Producer extends Thread {
    @Override
    public void run() {
      do {
        String msg = queue.peek();
        if (msg == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          continue;
        }
        System.out.println(msg);
        queue.poll();
      } while (true);
    }
  }
}
