/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.twitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import mis.patterns.TrackReducer;
import mis.trident.blueprints.state.GroupByField;
import org.apache.log4j.Logger;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TweetReducer implements ReducerAggregator<Map<String, Object>> {

    private static Logger log = Logger.getLogger(TweetReducer.class);

    public Map<String, Object> init() {
        return new HashMap<String, Object>();
    }

    public Map<String, Object> reduce(Map<String, Object> twitterUser, TridentTuple tuple) {
        long start = System.currentTimeMillis();
        if (twitterUser == null) {
            System.out.println("Input track object is null");
            twitterUser = new HashMap<String, Object>();
        }

//        System.out.println("Tuple size: "+tuple.size());
//        for(int i = 0; i < tuple.size(); i++){
//            System.out.println("Tuple index "+i+": "+tuple.get(i).toString());
//        }
        Map<String, Object> tweet = (Map<String, Object>) tuple.get(1);
        List<Map<String, Object>> tweets = (List<Map<String, Object>>) twitterUser.get("TWEETS");
        if (tweets == null) {
            tweets = new ArrayList<Map<String, Object>>();
            twitterUser.put("TWEETS", tweets);
        }        

        twitterUser.put("OBJECT_TYPE", "TWITTER_USER");
        twitterUser.put("USER_ID", tweet.get("TWEET_USER_ID"));


//        Double lat = Double.parseDouble(event.get("LATITUDE").toString());
//        Double lon = Double.parseDouble(event.get("LONGITUDE").toString());
        tweets.add(tweet);

        log.info("Twitter user: " + tweet.get("user_id") + " now has " + tweets.size() + " tweets");


//        long reduceTime = System.currentTimeMillis() - start;
//        log.info("It took " + reduceTime + " ms to process event " + uuid + " reduce step finished: " + System.nanoTime());
        return twitterUser;
    }
}
