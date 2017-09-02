package io.sugo.collect.parser;

import com.google.gson.*;
import io.sugo.collect.Configure;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.httpclient.*;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class LYParser extends CSVParser {

    private final Logger logger = LoggerFactory.getLogger(LYParser.class);
    public static final String PARSER_DIMENSIONS_ASSOCIATION_API = "parser.dimensions.association.api";
    private static final String ASSOCIATED_KEY = "activity_id";
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMdd hh:mm:ss");
    public String api;
    private Gson gson;
    private Map<String, JsonArray> associatedDimensions;
    private String exceptionDirectory;

    public LYParser(Configure conf) {
        super(conf);
        this.associatedDimensions = new HashMap<>();
        api = conf.getProperty(PARSER_DIMENSIONS_ASSOCIATION_API);
        gson = new Gson();
        associatedDimensions = requestDimensions(api);
        exceptionDirectory = conf.getProperty(Configure.USER_DIR) + "/ly/";
    }

    @Override
    public Map<String, Object> parse(String line) {
        Map<String, Object> map = new HashMap<>();
        if (this.associatedDimensions == null) {
            return map;
        }
        map = super.parse(line);
        String associatedKey = map.get(ASSOCIATED_KEY).toString();
        JsonArray dimensionArray = this.associatedDimensions.get(associatedKey);
        if (dimensionArray != null) {
            for (int i = 0;i < dimensionArray.size();i++) {
                JsonObject dimension = dimensionArray.get(i).getAsJsonObject();
                String key = dimension.get("key").getAsString();
                String value = dimension.get("value").getAsString();
                map.put(key, value);
            }
        } else {
            String exceptionPath = this.exceptionDirectory + associatedKey + ".log";
            File exceptionFile = new File(exceptionPath);
            try {
                FileUtils.writeStringToFile(exceptionFile, line + "\n", "UTF-8", true);
            } catch (IOException e) {
                logger.error("write to file failed: ", e);
            }
            return new HashMap<>();
        }
        String activityDateData = map.get("activity_date").toString();
        try {
            Date activityDate = this.dateFormat.parse(activityDateData);
            Long activityDateTimestamp = activityDate.getTime();
            map.put("activity_date", activityDateTimestamp);
        } catch (ParseException e) {
            logger.error("parse failed:  ", e);
        }
        return map;
    }

    private Map<String, JsonArray> requestDimensions(String url) {

        Map<String, JsonArray> dimensionsMap = new HashMap<>();
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);

        try {
            if (client.executeMethod(method) != HttpStatus.SC_OK) {
                logger.error("Method failed: ", method.getStatusLine());
            }
            String response = method.getResponseBodyAsString();
            JsonParser jsonParser = new JsonParser();
            JsonObject reponseObject = (JsonObject) jsonParser.parse(response);
            JsonArray resultArray = reponseObject.getAsJsonArray("result");
            for (int i = 0; i < resultArray.size(); i++) {
                JsonObject dimensionObject = (JsonObject) resultArray.get(i);
                String id = dimensionObject.get("id").getAsString();
                JsonArray valuesArray = dimensionObject.getAsJsonArray("values");
                dimensionsMap.put(id, valuesArray);
            }
            return dimensionsMap;
        } catch (Exception e) {
            logger.error("request dimensions failed: ", e);
            return null;
        } finally {
            method.releaseConnection();
        }
    }

}
