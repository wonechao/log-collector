package io.sugo.collect.reader.file;

import io.sugo.collect.Configure;
import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by fengxj on 4/8/17.
 */
public class DefaultFileReader extends AbstractReader {
  private final Logger logger = LoggerFactory.getLogger(DefaultFileReader.class);
  private Map<String, Reader> readerMap;
  public static final String FILE_READER_LOG_DIR = "file.reader.log.dir";
  public static final String COLLECT_OFFSET = ".collect_offset";
  public static final String FINISH_FILE = ".finish";
  public static final String FILE_READER_LOG_REGEX = "file.reader.log.regex";
  public static final String FILE_READER_SCAN_TIMERANGE = "file.reader.scan.timerange";
  public static final String FILE_READER_SCAN_INTERVAL = "file.reader.scan.interval";
  public static final String FILE_READER_THREADPOOL_SIZE = "file.reader.threadpool.size";

  private String metaBaseDir;
  ExecutorService fixedThreadPool;

  public DefaultFileReader(Configure conf, AbstractWriter writer) {
    super(conf, writer);
    readerMap = new HashMap<String, Reader>();
    int threadSize = conf.getInt(FILE_READER_THREADPOOL_SIZE);
    fixedThreadPool = Executors.newFixedThreadPool(threadSize);
  }

  @Override
  public void read() {
    metaBaseDir = System.getProperty("user.dir") + "/meta/";
    logger.info("DefaultFileReader started");
    int diffMin = conf.getInt(FILE_READER_SCAN_TIMERANGE);
    int inteval = conf.getInt(FILE_READER_SCAN_INTERVAL);
    long diffTs = diffMin  * 60l * 1000l;
    File directory = new File(conf.getProperty(FILE_READER_LOG_DIR));
    while (true) {
      addReader(directory);
      File[] files = directory.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
      long currentTime = System.currentTimeMillis();
      for (File subdir : files) {
        File metaDir = new File(metaBaseDir + "/" + subdir.getName());
        metaDir.mkdirs();
        long lastModTime = subdir.lastModified();
        //忽略过期目录
        if (currentTime - lastModTime > diffTs) {
          File finishFile = new File(metaDir, FINISH_FILE);
          try {
            if (!finishFile.exists())
              finishFile.createNewFile();
          } catch (IOException e) {
            logger.error("create file failed :" + FINISH_FILE, e);
          }
          continue;
        }

        File finishFile = new File(metaDir, FINISH_FILE);
        //忽略已完成目录
        if (!finishFile.exists())
          addReader(subdir);

      }

      try {
        Thread.sleep(inteval);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void addReader(File directory) {
    String directoryName = directory.getName();
    if (readerMap.containsKey(directoryName))
      return;

    Reader reader = new Reader(directory);
    fixedThreadPool.execute(reader);
    readerMap.put(directoryName, reader);
  }

  private class Reader implements Runnable {
    private final File directory;

    public Reader(File directory) {
      this.directory = directory;
    }

    @Override
    public void run() {
      logger.info("reading directory:" + directory.getAbsolutePath());
      try {
        int batchSize = conf.getInt(Configure.FILE_READER_BATCH_SIZE);
        File metaDir = new File(metaBaseDir + "/" + directory.getName());
        metaDir.mkdirs();
        File offsetFile = new File(metaDir + "/" + COLLECT_OFFSET);
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
          if (lastFileName != null && !lastFileName.equals(fileName))
            currentOffset = 0;

          long fileLength = randomAccessFile.length();
          //如果offset大于文件长度，从0开始读
          if (currentOffset > 0 && fileLength < currentOffset)
             currentOffset=0;


          logger.info("handle file:" + file.getAbsolutePath());
          randomAccessFile.seek(currentOffset);
          String tempString = null;
          int line = 0;
          List<String> messages = new ArrayList<>();
          do {
            tempString = randomAccessFile.readLine();
            //文件结尾处理
            if (tempString == null) {
              if (messages.size() > 0) {
                write(messages);
                lastFileOffset = currentOffset;
                //成功写入则记录消费位点，并继续读下一个文件
                FileUtils.writeStringToFile(offsetFile, fileName + ":" + lastFileOffset);
              }

              currentOffset = 0;
              StringBuffer logbuf = new StringBuffer();
              logbuf.append("file:").append(fileName).append("handle finished, total lines:").append(line);
              logger.info(logbuf.toString());
              break;
            }
            if (StringUtils.isNotBlank(tempString)) {
              tempString = new String(tempString.getBytes("ISO-8859-1"), "UTF-8");
              messages.add(tempString);
            }

            currentOffset = randomAccessFile.getFilePointer();
            line++;
            //分批写入
            if (line % batchSize == 0) {
              write(messages);
              lastFileOffset = currentOffset;
              FileUtils.writeStringToFile(offsetFile, fileName + ":" + lastFileOffset);
              messages = new ArrayList<>();
            }
            if (line % 10000 == 0) {
              long now = System.currentTimeMillis();
              long diff = now - current;
              current = now;
              StringBuffer logbuf = new StringBuffer("file:").append(file.getAbsolutePath()).append(" current line:")
                      .append(line).append(" time:").append(diff).append(" percent:").append((int)((double) currentOffset/(double)fileLength * 100)).append("%");
              logger.info(logbuf.toString());
            }
          } while (true);
        }
      } catch (Exception e) {
        logger.error("reader terminated abnormally ", e);
      } finally {
        readerMap.remove(directory.getName());
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
