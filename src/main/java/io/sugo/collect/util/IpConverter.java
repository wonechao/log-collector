package io.sugo.collect.util;

import io.sugo.collect.Configure;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by sugo on 17/6/21.
 */
public class IpConverter {
  private static final String DATA_SPLIT = "\\|";
  private static final String REGEX_IP = "^((\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3}))";
  private static final Pattern PATTERN_IP = Pattern.compile(REGEX_IP);
  private String ipFile;
  private final Set<String> needFields;
  private TreeMap<Long, Map<String, Object>> ipDetailMap;
  private static final CopyOnWriteArrayList<String> IP_LIB_FIELDS = new CopyOnWriteArrayList<String>(Arrays.asList(
      "sugo_nation",
      "sugo_province",
      "sugo_city",
      "sugo_district",
      "sugo_operator",
      "sugo_latitude",
      "sugo_longitude",
      "sugo_city_timezone",
      "sugo_timezone",
      "sugo_administrative",
      "sugo_phone_code",
      "sugo_nation_code",
      "sugo_continent",
      "sugo_area"));

  public IpConverter(String ipFile) {
    this(ipFile, null);
  }

  public IpConverter(String ipFile, Set<String> needFields) {
    this.ipFile = ipFile;
    this.needFields = needFields;
    initIpMap();
  }

  public Map<String, Object> getIpDetail(String ip) {
    if(null == ip || ip.isEmpty()) {
      return Collections.emptyMap();
    }
    long ipNumber = ipToNumber(ip);
    Map.Entry<Long, Map<String, Object>> nodeResult = this.ipDetailMap.higherEntry(ipNumber);
    //use the ternary operator will occur compiler error
    if(null == nodeResult) {
      return Collections.emptyMap();
    }
    return nodeResult.getValue();
  }

  private void initIpMap() {
    this.ipDetailMap = new TreeMap<>();
    try {
      InputStream inStream = getIpResource(this.ipFile);
      if(null != inStream) {
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
        String line = br.readLine();
        while (null != line) {
          String[] columns = line.split(DATA_SPLIT, -1);
          if(16 == columns.length) {
            Map<String, Object> libMap = new HashMap<>();
            for(int inx=0; inx < IP_LIB_FIELDS.size(); inx++) {
              String field = IP_LIB_FIELDS.get(inx);
              if (needFields == null || needFields.isEmpty() || needFields.contains(field))
                libMap.put(field, columns[inx + 2]);
            }
            long ipEnd = ipToNumber(columns[1]);
            this.ipDetailMap.put(ipEnd + 1, libMap);
          }
          line = br.readLine();
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private long ipToNumber(String ipStr) {
    long ipNumber = 0;
    Matcher matcher = PATTERN_IP.matcher(ipStr);
    if(matcher.find()) {
      if(5 == matcher.groupCount()) {
        ipNumber = (Long.parseLong(matcher.group(2)) << 24) + (Long.parseLong(matcher.group(3)) << 16) + (Long.parseLong(matcher.group(4)) << 8) + Long.parseLong(matcher.group(5));
      }
    }
    return ipNumber;
  }

  private InputStream getIpResource(String textName) throws FileNotFoundException {
      return new FileInputStream(textName);
  }
}
