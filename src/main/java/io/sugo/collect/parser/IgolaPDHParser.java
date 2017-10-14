package io.sugo.collect.parser;

import ch.qos.logback.core.encoder.ByteArrayUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.internal.StringMap;
import io.sugo.collect.Configure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IgolaPDHParser extends AbstractParser{
    private final Logger logger = LoggerFactory.getLogger(IgolaPDHParser.class);
    private static final Gson gson = new GsonBuilder().create();
    private static final String REQUEST = "request";
    private static final String RESPONSE = "response";
    public IgolaPDHParser(Configure conf) {
        super(conf);
    }

    @Override
    public Map<String, Object> parse(String line) throws Exception {
        Map<String,Object> tmpMap = gson.fromJson(line, Map.class);
        String topic = (String) tmpMap.get("@topic");
        long start =  Math.round((Double) tmpMap.get("@start"));
        Object contentObj = tmpMap.get("@content");
        StringMap<String> content = null;
        String tmpString;
        if (contentObj instanceof StringMap){
            content= (StringMap<String>) tmpMap.get("@content");
        }else if(contentObj instanceof ArrayList){
            //tmpString = uncompress((ArrayList<Double>) contentObj);
            //System.out.println(tmpString);
            throw new IgnorableException("snappy record");
        }

        String contentType = null;
        if(content.containsKey(REQUEST)){
            contentType = REQUEST;
        } else if(content.containsKey(RESPONSE)){
            contentType = RESPONSE;
        }

        if (contentType == null){
            return Collections.emptyMap();
        }

        Map<String,Object> res = new HashMap<String,Object>();
        res.put("start", start);
        res.put("topic", topic);
        res.put("content_type", contentType);

        String realContent = content.get(contentType);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode contentNode = objectMapper.readTree(realContent);
        switch (topic){
            case "pdh_create_session":
                res.putAll(parsePdhCreateSession(contentType, contentNode));
                break;
            case "pdh_separatedPolling":
            case "pdh_singlePolling":
            case "pdh_packagedPolling":
                res.putAll(parsePdhSeparatedPolling(contentType, contentNode));
                break;
            case "pdh_separatedByFuk":
            case "pdh_singleByFuk":
            case "pdh_packagedByFuk":
                res.putAll(parsePdhSeparatedByFuk(contentType, contentNode));
                break;
        }

        return res;
    }

    private Map<String, Object> parsePdhSeparatedByFuk(String contentType, JsonNode content) {
        Map<String, Object> res = new HashMap<String, Object>();
        if (REQUEST.equals(contentType)) {
            try{
                res.put("currency",content.get("currency").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("lang",content.get("lang").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("sorters",content.get("sorters").toString());
            } catch (NullPointerException e){
            }
            try{
                res.put("filters",content.get("filters").toString());
            } catch (NullPointerException e){
            }
            try{
                res.put("pageNumber",content.get("pageNumber").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("pageSize",content.get("pageSize").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("sessionId",content.get("sessionId").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("voyage",content.get("voyageInfo").get(0).get("voyage").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("fuk",content.get("voyageInfo").get(0).get("fuk").textValue());
            } catch (NullPointerException e){
            }

            res.put("traceId",content.get("@traceId").textValue());

            try{
                res.put("guid",content.get("headers").get("guid").get(0).textValue());
            }catch (NullPointerException e) {
            }

        }
        return res;
    }

    private Map<String, Object>  parsePdhSeparatedPolling(String contentType, JsonNode content) {
        Map<String, Object> res = new HashMap<String, Object>();
        if (REQUEST.equals(contentType)) {
            try{
                res.put("currency",content.get("currency").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("lang",content.get("lang").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("sorters",content.get("sorters").toString());
            } catch (NullPointerException e){
            }
            try{
                res.put("filters",content.get("filters").toString());
            } catch (NullPointerException e){
            }
            try{
                res.put("pageNumber",content.get("pageNumber").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("pageSize",content.get("pageSize").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("sessionId",content.get("sessionId").textValue());
            } catch (NullPointerException e){
            }
            res.put("traceId",content.get("@traceId").textValue());

            try{
                res.put("guid",content.get("headers").get("guid").get(0).textValue());
            }catch (NullPointerException e) {
            }

        }
        return res;
    }


    private Map<String, Object> parsePdhCreateSession(String contentType, JsonNode content) throws Exception {

        Map<String, Object> res = new HashMap<String, Object>();
        if (REQUEST.equals(contentType)) {
            try{
                res.put("lang",content.get("lang").textValue());
            } catch (NullPointerException e){
            }
            try{
                res.put("enableMagic",content.get("enableMagic").booleanValue());
            } catch (NullPointerException e){
            }try{
                res.put("magicEnabled",content.get("magicEnabled").booleanValue());
            } catch (NullPointerException e){
            }
            res.put("traceId",content.get("@traceId").textValue());
            JsonNode itemJsonNode = content.get("queryObj").get("item");
            if (itemJsonNode.isArray()){
                System.out.println(itemJsonNode.size());
                for (int i = 0; i < itemJsonNode.size(); i++) {
                    res.put("from_c_" + i,itemJsonNode.get(i).get("from").get("c").textValue());
                    res.put("from_t_"+ i,itemJsonNode.get(i).get("from").get("t").textValue());
                    res.put("to_c_"+ i,itemJsonNode.get(i).get("to").get("c").textValue());
                    res.put("to_t_"+ i,itemJsonNode.get(i).get("to").get("t").textValue());
                    res.put("date_"+ i,itemJsonNode.get(i).get("date").textValue());
                }
            }

        } else if (RESPONSE.equals(contentType)) {
            try {
                res.put("sessionId",content.get("sessionId").textValue());
                res.put("traceId",content.get("@traceId").textValue());
            }catch (NullPointerException e){
                throw new IgnorableException();
            }

        }
        return res;
    }

    private String uncompress(ArrayList<Double> intArray) throws IOException {
        int size = intArray.size();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) Math.round(intArray.get(i));
        }
        byte[] uncompressBytes = Snappy.uncompress(bytes);
        return new String(uncompressBytes, "UTF-8");
    }

    public static void main(String[] args) throws Exception {
        String line="{\"@tid\":\"111fe7b27de14880917ee02a70205a0c\",\"@rid\":\"11e8bc88fd4346539b9aa360c26f49ed\",\"@times\":0,\"@start\":1507298860981,\"@topic\":\"pdh_singleByFuk\",\"@content\":{\"request\":\"{\\\"filters\\\":[{\\\"key\\\":\\\"airline\\\",\\\"values\\\":[],\\\"voyage\\\":1},{\\\"key\\\":\\\"stops\\\",\\\"values\\\":[],\\\"voyage\\\":1},{\\\"key\\\":\\\"departureTime\\\",\\\"values\\\":[],\\\"voyage\\\":1},{\\\"key\\\":\\\"arrivalTime\\\",\\\"values\\\":[],\\\"voyage\\\":1},{\\\"key\\\":\\\"transferAirport\\\",\\\"values\\\":[],\\\"voyage\\\":1},{\\\"key\\\":\\\"departureAirport\\\",\\\"values\\\":[],\\\"voyage\\\":1},{\\\"key\\\":\\\"arrivalAirport\\\",\\\"values\\\":[],\\\"voyage\\\":1}],\\\"pageNumber\\\":1,\\\"pageSize\\\":60,\\\"sessionId\\\":\\\"a965cdd3-74e3-496b-b29a-086a83486789\\\",\\\"sorters\\\":[],\\\"voyage\\\":1,\\\"voyageInfo\\\":[{\\\"fuk\\\":\\\"FUK/JFK-XIY/20171225/MU298MU2152\\\",\\\"voyage\\\":0}],\\\"currency\\\":\\\"CNY\\\",\\\"lang\\\":\\\"ZH\\\",\\\"timestamp\\\":1507298860549,\\\"headers\\\":{\\\"Host\\\":[\\\"10.10.17.60:9146\\\"],\\\"Accept\\\":[\\\"application/json\\\"],\\\"Device-Id\\\":[\\\"TEV0217518003046\\\"],\\\"X-Real-IP\\\":[\\\"36.40.130.67\\\"],\\\"User-Agent\\\":[\\\"android mobile\\\"],\\\"Content-Type\\\":[\\\"application/json;charset\\u003dUTF-8\\\",\\\"application/x-www-form-urlencoded; charset\\u003dUTF-8\\\"],\\\"Version-Name\\\":[\\\"3.1.0\\\"],\\\"X-Original-To\\\":[\\\"10.10.17.20\\\"],\\\"Content-Length\\\":[\\\"560\\\"],\\\"Version-Number\\\":[\\\"56\\\"],\\\"Accept-Encoding\\\":[\\\"gzip\\\"],\\\"X-Forwarded-For\\\":[\\\"36.40.130.67, 36.40.130.67\\\",\\\"10.10.17.71\\\"],\\\"X-Forwarded-Proto\\\":[\\\"http\\\"]},\\\"@traceId\\\":\\\"3ed67e42-eb3e-44ab-b764-1dea71169433\\\"}\"},\"@compress\":false}";
        Map<String, Object> res = new IgolaPDHParser(new Configure()).parse(line);
        System.out.println(gson.toJson(res));

    }
}
