package io.sugo.collect;

import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.reader.ReaderFactory;
import io.sugo.collect.reader.file.DefaultFileReader;
import io.sugo.collect.writer.AbstractWriter;
import io.sugo.collect.writer.WriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;


/**
 * Created by fengxj on 4/8/17.
 */
public class LogCollector {
  private static final Logger logger = LoggerFactory.getLogger(LogCollector.class);
  public static void main(String[] args) throws Exception {
    Configure conf = new Configure();
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
