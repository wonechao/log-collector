package io.sugo.collect;

import io.sugo.collect.parser.GrokParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class GrokVerification {

    private static final Logger logger = LoggerFactory.getLogger(GrokVerification.class);

    public static void main(String[] args) throws Exception {

        if (args.length <= 0 || args[0].isEmpty()) {
            logger.error("Please input grok example!");
            return;
        }

        Configure configure = new Configure();
        Class parserClass = Class.forName(configure.getProperty(Configure.PARSER_CLASS));
        GrokParser grokParser = (GrokParser) parserClass.getDeclaredConstructor(new Class[]{Configure.class}).newInstance(configure);
        String filePath = args[0];
        File file = new File(filePath);
        FileInputStream fileInputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String example;
        while ((example = bufferedReader.readLine()) != null) {
            Map<String, Object> resultMap = grokParser.parse(example);
            logger.info("example:\n" + example);
            logger.info("result:\n" + resultMap.toString());
        }

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                logger.info("shutdown");
            }
        });

    }

}
