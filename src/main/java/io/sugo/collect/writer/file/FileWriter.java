package io.sugo.collect.writer.file;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;
import io.sugo.collect.writer.gateway.GatewayWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class FileWriter extends AbstractWriter {
  private BufferedWriter bufferWritter;
  private final Logger logger = LoggerFactory.getLogger(FileWriter.class);
  public FileWriter(Configure conf) {
    super(conf);
  }

  @Override
  public boolean write(List<String> messages) {
    for (String message : messages) {
      try {
        bufferWritter.write(message);
      } catch (IOException e) {
        logger.error("", e);
        return false;
      }
    }
    return true;
  }

  public FileWriter withFile(String filePath) {
    try {
      File logfile = new File(filePath);
      java.io.FileWriter fileWritter = new java.io.FileWriter(logfile, true);
      bufferWritter = new BufferedWriter(fileWritter);
      return this;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
