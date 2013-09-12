/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.twitter;

import backtype.storm.tuple.Values;
import be.datablend.blueprints.impls.mongodb.MongoDBGraph;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TwitterUserPersister extends BaseFunction {

    private static Logger log = Logger.getLogger(TwitterUserPersister.class);
    private Map<String, Vertex> userIds = new HashMap<String, Vertex>();
    private Graph graph = null;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context); //To change body of generated methods, choose Tools | Templates.
        if (conf.get("MONGO_HOST") != null && conf.get("MONGO_PORT") != null) {
            String mongoHost = conf.get("MONGO_HOST").toString();
            int mongoPort = Integer.parseInt(conf.get("MONGO_PORT").toString());
            graph = new MongoDBGraph(mongoHost, mongoPort);
        } else {
            graph = new MongoDBGraph("localhost", 27017);
        }

    }

    public void execute(TridentTuple tuple, TridentCollector collector) {

        List<Map<String, Object>> twitterUsers = (List<Map<String, Object>>) tuple.get(0);
        for (Map<String, Object> twitterUser : twitterUsers) {
            String userId = twitterUser.get("USER_ID").toString();

            if (!userIds.keySet().contains(userId)) {
                GraphQuery query = graph.query();
                query.has("OBJECT_TYPE", "TWITTER_USER");
                query.has("USER_ID", userId);
                Iterable<Vertex> vertices = query.vertices();
                int countFound = 0;
                for (Vertex v : vertices) {
                    userIds.put(userId, v);
                }

                if (countFound == 0) {
                    Vertex newVertex = graph.addVertex(null);
                    newVertex.setProperty("OBJECT_TYPE", "TWITTER_USER");
                    newVertex.setProperty("USER_ID", userId);
                    userIds.put(userId, newVertex);
                }
                closeIterable(vertices);
            }

            Vertex v = userIds.get(userId);

            List<Map<String, Object>> tweets = (List) v.getProperty("TWEETS");
            if (tweets == null) {
                tweets = new ArrayList<Map<String, Object>>();
            }
            //might have already persisted tweets, don't want to have tons of duplicates
            //so will persist one tweet per time stamp
            Set<String> tweetDates = new HashSet<String>();
            for (int i = 0; i < tweets.size(); i++) {
                tweetDates.add(tweets.get(i).get("tweet_created").toString());
            }
            List<Map<String, Object>> newTweets = (List) twitterUser.get("TWEETS");
            for (int i = 0; i < newTweets.size(); i++) {
                Map<String, Object> newTweet = newTweets.get(i);
                String date = newTweet.get("tweet_created").toString();
                if (!tweetDates.contains(date)) {
                    tweets.add(newTweet);
                    tweetDates.add(date);
                }
            }

            evaluateDistances(tweets, v);

            v.setProperty("TWEETS", tweets);
            
            if(v.getProperty("HAS_DISTANT_TWEETS")){
                collector.emit(new Values(v.getProperty("DISTANT_TWEETS")));
            }
        }
    }

    private void evaluateDistances(List<Map<String, Object>> tweets, Vertex twitterUser) {
        boolean foundDistantTweets = false;
        List<Map<String, Object>> distantTweetPairs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < tweets.size(); i++) {
            double lat1 = Double.parseDouble(tweets.get(i).get("lat").toString());
            double lon1 = Double.parseDouble(tweets.get(i).get("lon").toString());

            for (int j = i + 1; j < tweets.size(); j++) {
                double lat2 = Double.parseDouble(tweets.get(j).get("lat").toString());
                double lon2 = Double.parseDouble(tweets.get(j).get("lon").toString());

                double distance = calcDistance(lat1, lon1, lat2, lon2);
                if (distance > 200) {
                    System.out.println("Found distant tweet pair!");
                    foundDistantTweets = true;
                    Map<String, Object> distantTweetPair = new HashMap<String, Object>();
                    DateFormat twitterFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
                    String date1 = tweets.get(i).get("tweet_created").toString();
                    String date2 = tweets.get(j).get("tweet_created").toString();
                    String tweet1 = tweets.get(i).get("tweet_text").toString();
                    String tweet2 = tweets.get(j).get("tweet_text").toString();
                    try {
                        long time1 = twitterFormat.parse(date1).getTime();
                        long time2 = twitterFormat.parse(date2).getTime();
                       if (time1 > time2) {
                            distantTweetPair.put("firstTime", time2);
                            distantTweetPair.put("firstLat", lat2);
                            distantTweetPair.put("firstLon", lon2);
                            distantTweetPair.put("firstTweet", tweet2);
                            
                            distantTweetPair.put("secondTime", time1);
                            distantTweetPair.put("secondLat", lat1);
                            distantTweetPair.put("secondLon", lon1);
                            distantTweetPair.put("secondTweet", tweet1);
                        } else {
                            distantTweetPair.put("firstTime", time1);
                            distantTweetPair.put("firstLat", lat1);
                            distantTweetPair.put("firstLon", lon1);
                            distantTweetPair.put("firstTweet", tweet1);

                            distantTweetPair.put("secondTime", time2);
                            distantTweetPair.put("secondLat", lat2);
                            distantTweetPair.put("secondLon", lon2);
                            distantTweetPair.put("secondTweet", tweet2);
                        }
                        distantTweetPairs.add(distantTweetPair);
                    } catch (Exception e) {
                        log.error("Error parsing out distant tweet info", e);
                    }
                }

            }


        }

        twitterUser.setProperty("HAS_DISTANT_TWEETS", foundDistantTweets);
        twitterUser.setProperty("DISTANT_TWEETS", distantTweetPairs);

    }
    public static double EARTH_RADIUS = 6371.0;
    public static double KM_TO_NMI = 0.53996;

    public double calcDistance(double startLat, double startLon, double endLat, double endLon) {
        double distance = 0;
        double deltaLat = toRads((endLat - startLat));
        double deltaLon = toRads((endLon - startLon));

        double lat1 = toRads(startLat);
        double lat2 = toRads(endLat);

        double a = Math.sin(deltaLat / 2.0) * Math.sin(deltaLat / 2.0) + Math.sin(deltaLon / 2.0) * Math.sin(deltaLon / 2.0) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        distance = EARTH_RADIUS * c;
        //convert to NM
        distance *= KM_TO_NMI;

        return distance;
    }

    public double toRads(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private void closeIterable(Iterable it) {
        try {
            if (it instanceof CloseableIterable) {
                CloseableIterable closeable = (CloseableIterable) it;
                closeable.close();
            }
        } catch (Exception e) {
        }
    }
}
