package io.sugo.collect.reader.file;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.org.apache.bcel.internal.generic.FLOAD;
import io.sugo.collect.Configure;
import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.writer.AbstractWriter;
import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by fengxj on 4/8/17.
 */
public class BBKFileReader2 extends AbstractReader {
  private static final String UTF8 = "UTF-8";
  private final Logger logger = LoggerFactory.getLogger(BBKFileReader2.class);
  private Map<String, Reader> readerMap;
  public static final String FILE_READER_LOG_DIR = "file.reader.log.dir";
  public static final String COLLECT_OFFSET = ".collect_offset";
  public static final String FINISH_FILE = ".finish";
  public static final String FILE_READER_FILTER_REGEX = "file.reader.filter.regex";
  public static final String FILE_READER_LOG_REGEX = "file.reader.log.regex";
  public static final String FILE_READER_SCAN_TIMERANGE = "file.reader.scan.timerange";
  public static final String FILE_READER_SCAN_INTERVAL = "file.reader.scan.interval";
  public static final String FILE_READER_THREADPOOL_SIZE = "file.reader.threadpool.size";
  public static final String FILE_READER_HOST = "file.reader.host";
  public static final String FILE_READER_GROK_EXPR = "file.reader.grok.expr";


  private String host;
  private String metaBaseDir;
  ExecutorService fixedThreadPool;

  public BBKFileReader2(Configure conf, AbstractWriter writer) {
    super(conf, writer);
    host = conf.getProperty(FILE_READER_HOST);
    if (host == null)
      host = "";
    readerMap = new HashMap<String, Reader>();
    int threadSize = conf.getInt(FILE_READER_THREADPOOL_SIZE);
    fixedThreadPool = Executors.newFixedThreadPool(threadSize);
  }

  @Override
  public void read() {
    metaBaseDir = System.getProperty("user.dir") + "/meta/";
    logger.info("BBKFileReader started");
    int diffMin = conf.getInt(FILE_READER_SCAN_TIMERANGE);
    int inteval = conf.getInt(FILE_READER_SCAN_INTERVAL);
    long diffTs = diffMin * 60l * 1000l;
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
      Grok grok;
      try {
        grok = Grok.create("/Users/sugo/log-collector/src/main/resources/patterns");
        String grokExpr = conf.getProperty(FILE_READER_GROK_EXPR);
        grok.compile(grokExpr);
        if (StringUtils.isBlank(grokExpr)){
          logger.error(FILE_READER_GROK_EXPR + "must be set!");
          return;
        }
      } catch (GrokException e) {
        logger.error("", e);
        return;
      }
      String dirPath = directory.getAbsolutePath();
      logger.info("reading directory:" + dirPath);
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      try {
        int batchSize = conf.getInt(Configure.FILE_READER_BATCH_SIZE);
        File metaDir = new File(metaBaseDir + "/" + directory.getName());
        metaDir.mkdirs();
        File offsetFile = new File(metaDir + "/" + COLLECT_OFFSET);
        long lastFileOffset = 0;
        long lastByteOffset = 0;
        String lastFileName = null;
        String offsetStr = null;
        if (offsetFile.exists()) {
          offsetStr = FileUtils.readFileToString(offsetFile);
          String[] fields = StringUtils.split(offsetStr.trim(), ':');
          lastFileName = fields[0];
          lastFileOffset = Long.parseLong(fields[1]);
          lastByteOffset = Long.parseLong(fields[2]);
        }

        long currentOffset = lastFileOffset;
        long currentByteOffset = lastByteOffset;
        Collection<File> files = FileUtils.listFiles(directory, new SugoFileFilter(conf.getProperty(FILE_READER_LOG_REGEX), lastFileName), null);
        List<File> sortFiles = new ArrayList<>(files);
        Collections.sort(sortFiles, new Comparator<File>() {
          @Override
          public int compare(File o1, File o2) {
            return o1.getPath().compareTo(o2.getPath());
          }
        });
        long current = System.currentTimeMillis();
        for (File file : files) {
          String fileName = file.getName();

          //FileInputStream fis = new FileInputStream(file);
          if (lastFileName != null && !lastFileName.equals(fileName)) {
            currentByteOffset = 0;
            currentOffset = 0;
          }
          long fileLength = file.length();
          //如果offset大于文件长度，从0开始读
          if (currentByteOffset > 0 && fileLength < currentByteOffset) {
            currentOffset = 0;
            currentByteOffset = 0;
          }

          logger.info("handle file:" + file.getAbsolutePath());

          FileInputStream fis = new FileInputStream(file);
          fis.skip(currentByteOffset);
          BufferedReader br = new BufferedReader(
              new InputStreamReader(fis, Charset.forName(UTF8)));
          //br.skip(currentOffset);

          String tempString = null;
          int line = 0;
          int error = 0;
          List<String> messages = new ArrayList<>();
          do {
            tempString = br.readLine();

            //文件结尾处理
            if (tempString == null) {
              if (messages.size() > 0) {
                write(messages);
                //成功写入则记录消费位点，并继续读下一个文件
                FileUtils.writeStringToFile(offsetFile, fileName + ":" + currentOffset + ":" + currentByteOffset);
              }

              currentOffset = 0;
              currentByteOffset = 0;
              StringBuffer logbuf = new StringBuffer();
              logbuf.append("file:").append(fileName).append("handle finished, total lines:").append(line);
              logger.info(logbuf.toString());
              break;
            }
            if (StringUtils.isNotBlank(tempString)) {
              try {
                Match gm = grok.match(tempString);
                gm.captures();
                Map<String, Object> gmMap = gm.toMap();
                Map<String, Object> tmpMap = new HashMap<String, Object>();
                String type;
                for (String key: gmMap.keySet()) {
                  type = "s";
                  Object value = gmMap.get(key);
                  if (key.equals("time"))
                    type = "d";
                  else if (value instanceof Integer)
                    type = "i";
                  else if (value instanceof Long)
                    type = "l";
                  else if (value instanceof Float)
                    type = "f";

                  if (key.startsWith("json")){
                    String jsonStr = value.toString().replace("\\x5C","\\").replace("\\x22","\"").replace("\\x20", " ");
                    Map<String,Object> jsonMap = JSON.parseObject(jsonStr, Map.class);
                    tmpMap.putAll(jsonMap);
                    continue;
                  }
                  tmpMap.put(type + "|" + key, value);

                }
                messages.add(JSON.toJSONString(tmpMap));
              } catch (Exception e) {
                logger.error("failed to parse:" + tempString, e);
              }

            }

            currentOffset += (tempString.length() + 1);
            currentByteOffset += (tempString.getBytes(UTF8).length + 1);
            line++;
            //分批写入
            if (line % batchSize == 0) {
              write(messages);
              FileUtils.writeStringToFile(offsetFile, fileName + ":" + currentOffset + ":" + currentByteOffset);
              messages = new ArrayList<>();
            }

            if (line % 100000 == 0) {
              long now = System.currentTimeMillis();
              long diff = now - current;
              current = now;
              if (logger.isDebugEnabled()) {
                StringBuffer logbuf = new StringBuffer("file:").append(file.getAbsolutePath()).append(" current line:")
                    .append(line).append(" time:").append(diff).append(" percent:").append((int) ((double) currentByteOffset / (double) fileLength * 100)).append("%");
                logger.info(logbuf.toString());
                logger.info("error:" + error);
                logger.info("handle:" + line);
              }
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
    public String hex2Str(String str) throws UnsupportedEncodingException {
      String strArr[] = str.split("\\\\"); // 分割拿到形如 xE9 的16进制数据
      byte[] byteArr = new byte[strArr.length - 1];
      for (int i = 1; i < strArr.length; i++) {
        Integer hexInt = Integer.decode("0" + strArr[i]);
        byteArr[i - 1] = hexInt.byteValue();
      }
      return new String(byteArr, UTF8);
    }
  }
}
