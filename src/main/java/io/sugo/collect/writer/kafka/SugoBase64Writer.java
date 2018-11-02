package io.sugo.collect.writer.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sugo.collect.Configure;
import sun.misc.BASE64Encoder;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SugoBase64Writer extends KafkaWriter{
  private final Gson gson = new GsonBuilder().create();
  private static final String SUGO_TIMESTAMP = "sugo_timestamp";
  private static final String TAB = "\t";
  BASE64Encoder base64Encoder = new BASE64Encoder();
  public SugoBase64Writer(Configure conf) {
    super(conf);
  }

  @Override
  public boolean write(List<String> messages) {
    boolean res = true;
    for (String message: messages) {
      Map<String,Object> msgMap = gson.fromJson(message, Map.class);
      //todo: clean data

      StringBuffer columnStr = new StringBuffer();
      StringBuffer valStr = new StringBuffer();
      int fieldSize = msgMap.size();
      int countor = 0;
      for (String key: msgMap.keySet()) {
        columnStr.append(key);
        valStr.append(msgMap.get(key));
        if (countor ++ < fieldSize - 1){
          columnStr.append("\001");
          valStr.append("\001");
        }
      }

      columnStr.append("\002").append(valStr);
      String body = "";
      try {
        body = base64Encoder.encode(columnStr.toString().getBytes("UTF-8"));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }

      StringBuffer newMsg = new StringBuffer();
      long now = System.currentTimeMillis();
      if (msgMap.containsKey(SUGO_TIMESTAMP)){
        now = (long) msgMap.get(SUGO_TIMESTAMP);
      }

      newMsg.append(now).append(TAB)
          .append(TAB).append(TAB).append(TAB).append(TAB).append(TAB).append(TAB);
      newMsg.append(body);
      List<String> newMsgs = new ArrayList<>();
      newMsgs.add(newMsg.toString());

      for (String msg : newMsgs) {
        System.out.println(msg);
      }
      //if(!super.write(newMsgs)){
      //  res = false;
      //}
    }
    return res;
  }

}
