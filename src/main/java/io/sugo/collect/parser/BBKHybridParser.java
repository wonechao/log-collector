package io.sugo.collect.parser;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.sugo.collect.Configure;
import io.sugo.collect.util.AESUtil;
import io.sugo.collect.util.RSAUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BBKHybridParser extends GrokParser {

    private final Logger logger = LoggerFactory.getLogger(BBKHybridParser.class);
    private static final String HTTP_BASE_REQUEST_PARAM = "http_Base_Request_Param";
    private static final String EXCEPTION_KEY = "exception";
    public static final String FILE_READER_DECRYPTION_PRIVATE = "file.reader.decryption.private";
    private String privateKey;

    public BBKHybridParser(Configure conf) {
        super(conf);
        this.privateKey = conf.getProperty(FILE_READER_DECRYPTION_PRIVATE, "");
    }

    @Override
    public Map<String, Object> parse(String line) throws Exception {

        Map<String, Object> map = super.parse(line);
        if (map.isEmpty()) {
            throw new Exception("Parse failed: " + map);
        }
        String httpEncrypted = (String) map.get("http_encrypted");
        if (httpEncrypted.equals("encrypted")) {
            return encryptedParse(map);
        } else {
            return unencryptedParse(map);
        }
    }

    Map<String, Object> unencryptedParse(Map<String, Object> map) throws Exception {

        Gson gson = new Gson();
        //参考nginx日志模块ngx_http_log_escape方法
        //" \ del  会被转为\x22 \x5C \x7F
        //https://github.com/nginx/nginx/blob/9ad18e43ac2c9956399018cbb998337943988333/src/http/modules/ngx_http_log_module.c
        String httpBaseRequestParam = ((String) map.get(HTTP_BASE_REQUEST_PARAM)).replace("\\x5C", "\\").replace("\\x22", "\"");
        Map<String, String> httpBaseRequestParamMap = gson.fromJson(httpBaseRequestParam, new TypeToken<Map<String, String>>(){}.getType());
        for (String key : httpBaseRequestParamMap.keySet()) {
            map.put(key, httpBaseRequestParamMap.get(key));
        }
        map.remove(HTTP_BASE_REQUEST_PARAM);

        if (map.containsKey(EXCEPTION_KEY)) {
            Object exVal = map.get(EXCEPTION_KEY);
            map.put("exception_size", exVal.toString().length());
            map.put(EXCEPTION_KEY, exVal);
            map.remove(EXCEPTION_KEY);
        }
        return map;
    }

    Map<String, Object> encryptedParse(Map<String, Object> map) throws Exception {

        String pk = "";
        pk = RSAUtil.priDecrypt(map.get("http_eebbk_key").toString(), this.privateKey);
        String result = AESUtil.decryptAES(map.get(HTTP_BASE_REQUEST_PARAM).toString(), pk);
        Gson gson = new Gson();
        Map<String, String> httpBaseRequestParamMap = gson.fromJson(result, new TypeToken<Map<String, String>>(){}.getType());
        for (String key : httpBaseRequestParamMap.keySet()) {
            map.put(key, httpBaseRequestParamMap.get(key));
        }
        map.remove(HTTP_BASE_REQUEST_PARAM);

        if (map.containsKey(EXCEPTION_KEY)) {
            Object exVal = map.get(EXCEPTION_KEY);
            map.put("exception_size", exVal.toString().length());
            map.put(EXCEPTION_KEY, exVal);
            map.remove(EXCEPTION_KEY);
        }

        return map;
    }


}
