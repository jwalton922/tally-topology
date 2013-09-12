/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.twitter;

import backtype.storm.tuple.Values;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import mis.trident.blueprints.state.GroupByField;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TweetProcessor extends BaseFunction {

    private Set<String> tweetKeyWhiteList = new HashSet<String>();
    
    public TweetProcessor(){
        init();
    }
    
    private void init(){
        tweetKeyWhiteList.add("lat");
        tweetKeyWhiteList.add("lon");
        tweetKeyWhiteList.add("user_id");
        tweetKeyWhiteList.add("tweet_text");
        tweetKeyWhiteList.add("tweet_created");
    }
    
    private static Logger log = Logger.getLogger(TweetProcessor.class);

    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = new HashMap<String, Object>();
        if (tuple.get(0) instanceof byte[]) {
            byte[] messageBytes = (byte[]) tuple.get(0);
            String messageString = new String(messageBytes);
            System.out.println("flight delay tweet: " + messageString);

            try {
                JSONObject json = new JSONObject(messageString);
                System.out.println("Created json object");

                Iterator keyIt = json.keys();
                while (keyIt.hasNext()) {
                    String key = (String) keyIt.next();
                    if(!tweetKeyWhiteList.contains(key)){
                        continue;
                    }
                    Object value = json.get(key);
                    event.put(key, value);
                }
            } catch (Exception e) {
                System.out.println("Error parsing json from tweet object");
                log.error(e);
            }
            try {

                String id = event.get("user_id").toString();

                double lat = Double.parseDouble(event.get("lat").toString());
                double lon = Double.parseDouble(event.get("lon").toString());


                if (lat > 0 || lat < 0) {
                    MessageDigest md = MessageDigest.getInstance("SHA-256");
                    md.update(id.getBytes());

                    byte byteData[] = md.digest();

                    //convert the byte to hex format method 1
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < byteData.length; i++) {
                        sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
                    }
                    id = sb.toString();
                    GroupByField idField = new GroupByField("TWEET_USER_ID", id);
                    GroupByField objectType = new GroupByField("OBJECT_TYPE", "TWITTER_USER");
                    event.put("TWEET_USER_ID", id);   
                    event.put("user_id", id);
                    
                    collector.emit(new Values(idField, objectType, event));
                }

            } catch (Exception e) {
                log.error("Error parsing tweet info", e);
            }

        } else {
            System.out.println("object at tuple index 0: " + tuple.get(0).getClass().getName());
        }

    }
}
