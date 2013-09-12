/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.twitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import mis.patterns.TrackObjectOutputter;
import mis.trident.blueprints.state.SerializableMongoDBGraph;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TweetOutputter extends BaseFunction {

    private static Logger log = Logger.getLogger(TrackObjectOutputter.class);
    private JedisPoolConfig poolConfig;
    private JedisPool jedisPool;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        String redisHost = (String) conf.get("REDIS_HOST");
        Integer redisPort = Integer.parseInt(conf.get("REDIS_PORT").toString());
        poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 0);
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = new HashMap<String, Object>();
        if (tuple.get(0) instanceof byte[]) {
            byte[] messageBytes = (byte[]) tuple.get(0);
            String messageString = new String(messageBytes);
            System.out.println("flight delay tweet: " + messageString);

            try {
                JSONObject json = new JSONObject(messageString);
                System.out.println("Created json object");
                String text = json.getString("tweet_text").toLowerCase();
                if (text.indexOf("flight") >= 0 && text.indexOf("delay") >= 0) {
                    Jedis jedis = jedisPool.getResource();
                    Long numClients = jedis.publish("FLIGHT_DELAY_TWEET", messageString);
                    jedisPool.returnResource(jedis);
                }
//                Iterator keyIt = json.keys();
//                while (keyIt.hasNext()) {
//                    String key = (String) keyIt.next();
//                    Object value = json.get(key);
//                    if (value instanceof JSONArray) {
//                        JSONArray valueArray = (JSONArray) value;
//                        List<Object> valueList = new ArrayList<Object>();
//                        Map<String, Object> innerMap = new HashMap<String, Object>();
//                        for (int i = 0; i < valueArray.length(); i++) {
//                            JSONObject listObject = valueArray.getJSONObject(i);
//                            Iterator innerKeyIt = listObject.keys();
//                            while (innerKeyIt.hasNext()) {
//                                String innerKey = (String) innerKeyIt.next();
//                                Object innerValue = listObject.get(innerKey);
//                                innerMap.put(innerKey, innerValue);
//                            }
//                            valueList.add(innerMap);
//                        }
//
//                        value = valueList;
//                    }
//                }
            } catch (Exception e) {
                System.out.println("Error parsing json from tweet object");
                log.error(e);
            }
        } else {
            System.out.println("object at tuple index 0: " + tuple.get(0).getClass().getName());
        }

    }
}
