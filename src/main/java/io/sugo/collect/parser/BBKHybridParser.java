package io.sugo.collect.parser;

import io.sugo.collect.Configure;
import io.sugo.collect.util.AESUtil;
import io.sugo.collect.util.RSAUtil;
import io.sugo.grok.api.Grok;
import io.sugo.grok.api.Match;
import io.sugo.grok.api.exception.GrokException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BBKHybridParser extends GrokParser {

    private final Logger logger = LoggerFactory.getLogger(BBKHybridParser.class);
    public static final String FILE_READER_GROK_PATTERNS_PATH = "file.reader.grok.patterns.path";
    public static final String FILE_READER_DECRYPTION_PRIVATE = "file.reader.decryption.private";
    public static final String FILE_READER_GROK_EXPR = "file.reader.grok.expr";
    private static final String EXCEPTION_KEY = "exception";
    private Grok grok;
    private String privateKey;

    public BBKHybridParser(Configure conf) {
        super(conf);
        try {
            String patternPath = conf.getProperty(FILE_READER_GROK_PATTERNS_PATH);
            if (StringUtils.isBlank(patternPath)){
                patternPath = conf.getProperty(Configure.USER_DIR) + "conf/patterns";
            }
            if (!patternPath.startsWith("/"))
                patternPath = conf.getProperty(Configure.USER_DIR) + "/" + patternPath;
            logger.info("final patternPath:" + patternPath);
            grok = Grok.create(patternPath);
            String grokExpr = conf.getProperty(FILE_READER_GROK_EXPR);
            if (StringUtils.isBlank(grokExpr)){
                logger.error(FILE_READER_GROK_EXPR + "must be set!");
                System.exit(1);
            }
            grok.compile(grokExpr);
        } catch (GrokException e) {
            logger.error("", e);
            System.exit(1);
        }
        this.privateKey = conf.getProperty(FILE_READER_DECRYPTION_PRIVATE, "");
    }

    @Override
    public Map<String, Object> parse(String line) throws Exception {

        Map<String, Object> map = super.parse(line);
        String httpEncrypted = (String) map.get("http_encrypted");
        if (httpEncrypted.equals("encrypted")) {
            return encryptedParse(line);
        } else {
            return unencryptedParse(line);
        }
    }

    Map<String, Object> unencryptedParse(String line) throws Exception {

        //参考nginx日志模块ngx_http_log_escape方法
        //" \ del  会被转为\x22 \x5C \x7F
        //https://github.com/nginx/nginx/blob/9ad18e43ac2c9956399018cbb998337943988333/src/http/modules/ngx_http_log_module.c
        line = line.replace("\\x5C", "\\").replace("\\x22", "\"");
        Map<String, Object> map = super.parse(line);
        if (map.containsKey(EXCEPTION_KEY)) {
            Object exVal = map.get(EXCEPTION_KEY);
            map.put("exception_size", exVal.toString().length());
            map.put(EXCEPTION_KEY, exVal);
            map.remove(EXCEPTION_KEY);
        }
        return map;
    }

    Map<String, Object> encryptedParse(String line) throws Exception {
        Match gm = this.grok.match(line);
        gm.captures();
        Map<String, Object> map = gm.toMap();
        String key = "";
        key = RSAUtil.priDecrypt(map.get("http_eebbk_key").toString(), this.privateKey);
        String result = AESUtil.decryptAES(map.get("http_Base_Request_Param").toString(), key);
        map.put("http_Base_Request_Param", result);
        return map;
    }


}
