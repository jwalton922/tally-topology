/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import com.mongodb.util.JSON;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import mis.trident.blueprints.state.SerializableMongoDBGraph;
import org.apache.log4j.Logger;
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
public class TrackObjectOutputter extends BaseFunction {

    private static Logger log = Logger.getLogger(TrackObjectOutputter.class);
    private JedisPoolConfig poolConfig;
    private JedisPool jedisPool;
    private SerializableMongoDBGraph graph;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        String redisHost = (String) conf.get("REDIS_HOST");
        Integer redisPort = Integer.parseInt(conf.get("REDIS_PORT").toString());
        poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 0);
    }

    private void init(Map conf, SerializableMongoDBGraph graph) {
        this.graph = graph;
        String redisHost = (String) conf.get("REDIS_HOST");
        Integer redisPort = Integer.parseInt(conf.get("REDIS_PORT").toString());
        poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 0);
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        long start = System.currentTimeMillis();
        log.debug("TrackObjectOutputter excecute called on tuple of size: " + tuple.size());
        
        Long currentTime = System.currentTimeMillis();
//        for (int i = 0; i < tuple.size(); i++) {
//            if (tuple.get(i) != null) {
//                log.debug("TrackObjectOutputter Tuple value at index " + i + ": " + tuple.get(i).toString());
//            } else {
//                log.debug("TrackObjectOutputter Tuple value at index " + i + ": null");
//            }
//        }

        List<Map<String, Object>> tracks = (List) tuple.get(0);
        List<Map<String, Object>> lastPositionList = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < tracks.size(); i++) {
            Map<String, Object> track = tracks.get(i);
            if(track == null){
                continue;
            }
            List<Map<String, Object>> positions = (List<Map<String, Object>>) track.get("POSITIONS");
            if (positions == null || positions.size() == 0) {
                continue;
            }

            Map<String, Object> lastPosition = positions.get(0);

            Long lastTime = (Long) lastPosition.get("TIME");
//            Double lat = (Double) position.get("LATITUDE");
//            Double lon = (Double) position.get("LONGITUDE");
            for (int j = 1; j < positions.size(); j++) {
                Long time = (Long) positions.get(j).get("TIME");
                if (time > lastTime) {
                    lastPosition = positions.get(j);
                    lastTime = time;
                }
            }
            //if position is less than 5 minutes old add it;
            if ((currentTime - lastTime) < 300000) {
                lastPositionList.add(lastPosition);
            }
        }

        String lastPositionJson = JSON.serialize(lastPositionList);

        Jedis jedis = jedisPool.getResource();
        Long numClients = jedis.publish("TRACKS", lastPositionJson);
        jedisPool.returnResource(jedis);
        long time = System.currentTimeMillis() - start;
        log.debug("Objects published: " + lastPositionList.size() + " Clients receiving last track publishing: " + numClients + " time " + time + " ms to query and publish");
    }

    public void publishObjects() {
        long start = System.currentTimeMillis();
        log.debug("publishObjects called");
        Long currentTime = System.currentTimeMillis();
        GraphQuery query = graph.getGraph().query();
        query.has("OBJECT_TYPE", "TRACK");
        Iterable<Vertex> vertices = query.vertices();
        List<Map<String, Object>> lastPositionList = new ArrayList<Map<String, Object>>();
        for (Vertex v : vertices) {
            Map<String, Object> track = v.getProperty("TRIDENT_VALUE");
            List<Map<String, Object>> positions = (List<Map<String, Object>>) track.get("POSITIONS");
            if (positions.size() == 0) {
                continue;
            }

            Map<String, Object> lastPosition = positions.get(0);
            Long lastTime = (Long) lastPosition.get("TIME");
//            Double lat = (Double) position.get("LATITUDE");
//            Double lon = (Double) position.get("LONGITUDE");
            for (int i = 1; i < positions.size(); i++) {
                Long time = (Long) positions.get(i).get("TIME");
                if (time > lastTime) {
                    lastPosition = positions.get(i);
                    lastTime = time;
                }
            }
            //if position is less than 5 minutes old add it;
            if ((currentTime - lastTime) < 300000) {
                lastPositionList.add(lastPosition);
            }
        }

        String lastPositionJson = JSON.serialize(lastPositionList);

        Jedis jedis = jedisPool.getResource();
        Long numClients = jedis.publish("TRACKS", lastPositionJson);
        jedisPool.returnResource(jedis);
        long time = System.currentTimeMillis() - start;
        log.debug("Objects published: " + lastPositionList.size() + " Clients receiving last track publishing: " + numClients + " time " + time + " ms to query and publish");

    }

    public static void main(String[] args) throws Exception {
        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);
        TrackObjectOutputter objectOutputter = new TrackObjectOutputter();
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("REDIS_HOST", "localhost");
        conf.put("REDIS_PORT", 6379);

        objectOutputter.init(conf, graph);
        int timesPublished = 0;
        while (true) {
            objectOutputter.publishObjects();
            timesPublished++;
            log.debug("Times published = " + timesPublished);
            Thread.sleep(1000);
        }
//        log.debug("Done publishing objects");
    }
}
