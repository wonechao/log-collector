package io.sugo.collect.reader.file;

import io.sugo.collect.Configure;
import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;

/**
 * Created by fengxj on 4/8/17.
 */
public class DefaultFileReader extends AbstractReader {

  public static final String FILE_READER_LOG_DIR = "file.reader.log.dir";
  public static final String COLLECT_OFFSET = "collect_offset";
  public static final String FILE_READER_LOG_SUFFIX = "file.reader.log.suffix";

  public DefaultFileReader(Configure conf, AbstractWriter writer) {
    super(conf, writer);
  }

  @Override
  public void read() {
    new Reader().start();
  }

  private class Reader extends Thread {
    @Override
    public void run() {
      while (true) {
        File directory = new File(conf.getProperty(FILE_READER_LOG_DIR));
        File offsetFile = new File(directory, COLLECT_OFFSET);
        try {
          long lastFileOffset = 0;
          String lastFileName = null;
          if (offsetFile.exists()) {
            String offsetStr = FileUtils.readFileToString(offsetFile);
            String[] fields = StringUtils.split(offsetStr.trim(), ':');
            lastFileName = fields[0];
            lastFileOffset = Long.parseLong(fields[1]);
          }

          Collection<File> files = FileUtils.listFiles(directory, new String[]{conf.getProperty(FILE_READER_LOG_SUFFIX)}, false);
          for (File file : files) {
            if (lastFileName != null) {
              String fileName = file.getName();
              if (lastFileName.compareTo(fileName) > 0) {
                continue;
              }
            }

            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(lastFileOffset);
            String tempString = null;
            int line = 1;
            do {
              tempString = randomAccessFile.readLine();
              if (tempString == null)
                break;
              boolean res = writer.write(tempString);
              if (res) {
                lastFileOffset = randomAccessFile.getFilePointer();
                if (line % 10000 == 0) {
                  FileUtils.writeStringToFile(offsetFile, file.getName() + ":" + lastFileOffset);
                }
                line++;
              } else {
                //发送失败则重试
                randomAccessFile.seek(lastFileOffset);
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            } while (true);
            FileUtils.writeStringToFile(offsetFile, file.getName() + ":" + lastFileOffset);
            lastFileOffset = 0;
          }
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
