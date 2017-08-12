package io.sugo.collect;

import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.reader.ReaderFactory;
import io.sugo.collect.writer.AbstractWriter;
import io.sugo.collect.writer.WriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;


/**
 * Created by fengxj on 4/8/17.
 */
public class LogCollector {
  private static final Logger logger = LoggerFactory.getLogger(LogCollector.class);
  public static void main(String[] args) throws Exception {
    Configure conf = new Configure();
    String logfile = conf.getProperty(Configure.USER_DIR) + "/.lock";
    FileChannel channel = new FileOutputStream(logfile, true).getChannel();
    FileLock lock = channel.tryLock();
    if(lock == null) {
      logger.warn("Another process is running, please stop it first!");
     return;
    }

    AbstractWriter writer = new WriterFactory(conf).createWriter();
    final AbstractReader reader = new ReaderFactory(conf).createReader(writer);

    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        logger.info("shutdown now...");
        reader.stop();
        logger.info("shutdown successfully");
      }
    });

    reader.read();

  }
}
