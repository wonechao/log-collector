package io.sugo.collect.reader.file;

import io.sugo.collect.Configure;
import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by fengxj on 4/8/17.
 */
public class DefaultFileReader extends AbstractReader {
  private final Logger logger = LoggerFactory.getLogger(DefaultFileReader.class);
  public static final String FILE_READER_LOG_DIR = "file.reader.log.dir";
  public static final String COLLECT_OFFSET = ".collect_offset";
  public static final String FILE_READER_LOG_REGEX = "file.reader.log.regex";

  public DefaultFileReader(Configure conf, AbstractWriter writer) {
    super(conf, writer);
  }

  @Override
  public void read() {
    new Reader().start();
    logger.info("DefaultFileReader started");
  }

  private class Reader extends Thread {

    @Override
    public void run() {
      String userDir = System.getProperty("user.dir");
      logger.info("user.dir:" + userDir);
      while (true) {
        int batchSize = conf.getInt(Configure.FILE_READER_BATCH_SIZE);
        File directory = new File(conf.getProperty(FILE_READER_LOG_DIR));
        File offsetFile = new File(userDir + "/" + COLLECT_OFFSET);


        try {
          long lastFileOffset = 0;
          String lastFileName = null;
          String offsetStr = null;
          if (offsetFile.exists()) {
            offsetStr = FileUtils.readFileToString(offsetFile);
            String[] fields = StringUtils.split(offsetStr.trim(), ':');
            lastFileName = fields[0];
            lastFileOffset = Long.parseLong(fields[1]);
          }

          long currentOffset = lastFileOffset;
          Collection<File> files = FileUtils.listFiles(directory, new SugoFileFilter(conf.getProperty(FILE_READER_LOG_REGEX), lastFileName, lastFileOffset), null);
          long current = System.currentTimeMillis();
          for (File file : files) {
            String fileName = file.getName();

            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            //如果offset为文件尾部，直接读下一个文件
            if (randomAccessFile.length() == lastFileOffset) {
              currentOffset = 0;
              continue;
            }


            logger.info("handle file:" + fileName);
            randomAccessFile.seek(currentOffset);
            String tempString = null;
            int line = 0;
            List<String> messages = new ArrayList<>();
            do {
              //long bufLength = buf.length();

              tempString = randomAccessFile.readLine();
              //文件结尾处理
              if (tempString == null) {
                if (messages.size() == 0) {
                  currentOffset = 0;
                  break;
                }

                write(messages);

                lastFileOffset = currentOffset;
                //成功写入则记录消费位点，并继续读下一个文件
                FileUtils.writeStringToFile(offsetFile, file.getName() + ":" + lastFileOffset);
                messages = new ArrayList<>();
                currentOffset = 0;
                StringBuffer logbuf = new StringBuffer();
                logbuf.append("file:").append(fileName).append("handle finished, total lines:").append(line);
                logger.info(logbuf.toString());
                break;
              }
              if (StringUtils.isNotBlank(tempString)) {
                tempString = new String(tempString.getBytes("ISO-8859-1"), "UTF-8");
                messages.add(tempString);
                //buf.append(tempString);
              }

              currentOffset = randomAccessFile.getFilePointer();
              line++;
              //分批写入
              if (line % batchSize == 0) {
                write(messages);
                lastFileOffset = currentOffset;
                FileUtils.writeStringToFile(offsetFile, file.getName() + ":" + lastFileOffset);
                messages = new ArrayList<>();
              }
              if (line % 10000 == 0) {
                long now = System.currentTimeMillis();
                long diff = now - current;
                current = now;
                logger.info("current line:" + line + " time:" + diff);
              }
            } while (true);
          }
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    private boolean write(List<String> messages) throws InterruptedException {
      boolean res = writer.write(messages);
      if (!res) {
        logger.warn("写入失败，1秒后重试!!!");
        Thread.sleep(1000);
        return writer.write(messages);
      }
      return false;
    }
  }
}
