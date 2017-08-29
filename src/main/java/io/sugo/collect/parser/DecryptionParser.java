package io.sugo.collect.parser;

import io.sugo.collect.Configure;
import io.sugo.collect.util.AESUtil;
import io.sugo.collect.util.RSAUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.sugo.grok.api.Grok;
import io.sugo.grok.api.Match;
import io.sugo.grok.api.exception.GrokException;


import java.util.Map;

public class DecryptionParser extends AbstractParser {

    private final Logger logger = LoggerFactory.getLogger(DecryptionParser.class);
    public static final String FILE_READER_GROK_PATTERNS_PATH = "file.reader.grok.patterns.path";
    public static final String FILE_READER_DECRYPTION_PRIVATE = "file.reader.decryption.private";
    public static final String FILE_READER_GROK_EXPR = "file.reader.grok.expr";
    private Grok grok;
    private String privateKey;

    public DecryptionParser(Configure conf) {
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
        Match gm = this.grok.match(line);
        gm.captures();
        Map<String, Object> map = gm.toMap();
        String key = "";
        key = RSAUtil.priDecrypt(map.get("s_key").toString(), this.privateKey);
        String result = AESUtil.decryptAES(map.get("json_base_request").toString(), key);
        map.put("json_base_request", result);
        return map;
    }

}
