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

    private Gson gson = new Gson();

    public BBKHybridParser(Configure conf) {
        super(conf);
        this.privateKey = conf.getProperty(FILE_READER_DECRYPTION_PRIVATE, "");
    }

    @Override
    public Map<String, Object> parse(String line) throws Exception {

        Map<String, Object> originalMap = super.parse(line);
        if (originalMap.isEmpty()) {
            throw new Exception("Parse failed");
        }
        String httpEncrypted = (String) originalMap.get("http_encrypted");
        if (httpEncrypted.equals("encrypted")) {
            encryptedParse(originalMap);
        } else {
            unencryptedParse(originalMap);
        }

        String result = (String)originalMap.get(HTTP_BASE_REQUEST_PARAM);
        Map<String, String> httpBaseRequestParamMap = gson.fromJson(result, new TypeToken<Map<String, String>>(){}.getType());
        for (String key : httpBaseRequestParamMap.keySet()) {
            originalMap.put(key, httpBaseRequestParamMap.get(key));
        }
        originalMap.remove(HTTP_BASE_REQUEST_PARAM);

        if (originalMap.containsKey(EXCEPTION_KEY)) {
            Object exVal = originalMap.get(EXCEPTION_KEY);
            originalMap.put("exception_size", exVal.toString().length());
            originalMap.put(EXCEPTION_KEY, exVal);
            originalMap.remove(EXCEPTION_KEY);
        }
        return originalMap;
    }

    private void unencryptedParse(Map<String, Object> map) throws Exception {
        //参考nginx日志模块ngx_http_log_escape方法
        //" \ del  会被转为\x22 \x5C \x7F
        //https://github.com/nginx/nginx/blob/9ad18e43ac2c9956399018cbb998337943988333/src/http/modules/ngx_http_log_module.c
        String httpBaseRequestParam = ((String) map.get(HTTP_BASE_REQUEST_PARAM)).replace("\\x5C", "\\").replace("\\x22", "\"");
        map.put(HTTP_BASE_REQUEST_PARAM, httpBaseRequestParam);
    }

    private void encryptedParse(Map<String, Object> map) throws Exception {
        String pk = RSAUtil.priDecrypt(map.get("http_eebbk_key").toString(), this.privateKey);
        String result = AESUtil.decryptAES(map.get(HTTP_BASE_REQUEST_PARAM).toString(), pk);
        map.put(HTTP_BASE_REQUEST_PARAM, result);
    }


}
