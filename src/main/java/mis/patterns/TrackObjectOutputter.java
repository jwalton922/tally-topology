/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import backtype.storm.tuple.Values;
import com.mongodb.util.JSON;
import java.util.ArrayList;
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
        System.out.println("TrackObjectOutputter excecute called on tuple of size: " + tuple.size());
        log.info("TrackObjectOutputter excecute called on tuple of size: " + tuple.size());

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
        log.info("There are " + tracks.size() + " tracks to process");
        for (int i = 0; i < tracks.size(); i++) {
            Map<String, Object> track = tracks.get(i);
            if (track == null) {
                log.info("Track is null!");
                continue;
            }
            List<Map<String, Object>> positions = (List<Map<String, Object>>) track.get("POSITIONS");
            if (positions == null || positions.size() == 0) {
                log.info("No positions!");
                continue;
            } else {
                log.info("Track has " + positions.size() + " positions, last position: " + positions.get(positions.size() - 1).toString());
            }

            Map<String, Object> lastPosition = positions.get(0);

            Long lastTime = (Long) lastPosition.get("TIME");
            Double lat = Double.parseDouble(lastPosition.get("LATITUDE").toString());
            Double lon = Double.parseDouble(lastPosition.get("LONGITUDE").toString());
//            Double lat = (Double) position.get("LATITUDE");
//            Double lon = (Double) position.get("LONGITUDE");
            for (int j = 1; j < positions.size(); j++) {
                Long time = (Long) positions.get(j).get("TIME");
                if (time > lastTime) {
                    lastPosition = positions.get(j);
                    lastTime = time;
                    lat = Double.parseDouble(lastPosition.get("LATITUDE").toString());
                    lon = Double.parseDouble(lastPosition.get("LONGITUDE").toString());
                }
            }
            //if position is less than 5 minutes old add it;
            long diff = (currentTime - lastTime);
            if (diff < 900000) {
                log.info("Found a last position");
                processSchedule(track, lastPosition);
                lastPositionList.add(lastPosition);
            } else {
                log.info("diff = " + diff + " currentTime = " + currentTime + " lastTime = " + lastTime);
            }
        }

        String lastPositionJson = JSON.serialize(lastPositionList);

        Jedis jedis = jedisPool.getResource();
        Long numClients = jedis.publish("TRACKS", lastPositionJson);
        jedisPool.returnResource(jedis);
        long time = System.currentTimeMillis() - start;
        log.info("Objects published: " + lastPositionList.size() + " Clients receiving last track publishing: " + numClients + " time " + time + " ms to query and publish");
        System.out.println("Objects published: " + lastPositionList.size() + " Clients receiving last track publishing: " + numClients + " time " + time + " ms to query and publish");
        //collector.emit(new Values("finished"));
    }

    public void processSchedule(Map<String, Object> track, Map<String, Object> currentPosition) {
        log.info("process schedule called");
        if (track.get("SCHEDULE") != null) {
            List<Map<String, Object>> scheduleList = (List) track.get("SCHEDULE");
            Long currentTime = (Long) currentPosition.get("TIME");
            for (int i = 0; i < scheduleList.size(); i++) {
                //"SCHEDULE":[{"ARRIVAL_TIME":1378824720000,"DEPARTURE_TIME":1378816440000,"DESTINATION":"Arnold Palmer Rgnl (KLBE)","DESTINATION_LONGITUDE":-79.40675,"DESTINATION_LATITUDE":40.27461111111111,"ORIGIN":"Dallas/Fort Worth Intl (KDFW)"}]
                Map<String, Object> schedule = scheduleList.get(i);
                Long arrivalTime = (Long) schedule.get("ARRIVAL_TIME");
                Long departureTime = (Long) schedule.get("DEPARTURE_TIME");
                if (currentTime < arrivalTime && currentTime > departureTime) {
                    Double destLat = Double.parseDouble(schedule.get("DESTINATION_LATITUDE").toString());
                    Double destLon = Double.parseDouble(schedule.get("DESTINATION_LONGITUDE").toString());
                    Double lat = Double.parseDouble(currentPosition.get("LATITUDE").toString());
                    Double lon = Double.parseDouble(currentPosition.get("LONGITUDE").toString());

                    double distance = calcDistance(lat, lon, destLat, destLon);
                    try {
                        Double speed = Double.parseDouble(currentPosition.get("SPEED").toString());
                        //speed is in knots
                        double time = distance / (speed*1.0);
                        //convert to hours to ms
                        long timeInMs = (long)(time*60*60*1000);
                        long timeToArrival = arrivalTime - currentTime;
                        System.out.println("Time to arrival: "+timeToArrival+" calculated time to arrival: "+timeInMs);
                        if(timeInMs > timeToArrival){
                            log.info("FOUND A DELAYED FLIGHT!!!!");
                            currentPosition.put("DELAYED", true);
                            currentPosition.put("DELAYED_TIME", (timeInMs - timeToArrival));
                        } else {
                            currentPosition.put("DELAYED", false);
                        }
                        
                    } catch (Exception e) {
                        log.info("Error parsing speed: " + currentPosition.get("SPEED"));
                    }
                }
            }
        }
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
//    public void publishObjects() {
//        long start = System.currentTimeMillis();
//        log.debug("publishObjects called");
//        Long currentTime = System.currentTimeMillis();
//        GraphQuery query = graph.getGraph().query();
//        query.has("OBJECT_TYPE", "TRACK");
//        Iterable<Vertex> vertices = query.vertices();
//        List<Map<String, Object>> lastPositionList = new ArrayList<Map<String, Object>>();
//        for (Vertex v : vertices) {
//            Map<String, Object> track = v.getProperty("TRIDENT_VALUE");
//            List<Map<String, Object>> positions = (List<Map<String, Object>>) track.get("POSITIONS");
//            if (positions.size() == 0) {
//                continue;
//            }
//
//            Map<String, Object> lastPosition = positions.get(0);
//            Long lastTime = (Long) lastPosition.get("TIME");
////            Double lat = (Double) position.get("LATITUDE");
////            Double lon = (Double) position.get("LONGITUDE");
//            for (int i = 1; i < positions.size(); i++) {
//                Long time = (Long) positions.get(i).get("TIME");
//                if (time > lastTime) {
//                    lastPosition = positions.get(i);
//                    lastTime = time;
//                }
//            }
//            //if position is less than 5 minutes old add it;
//            if ((currentTime - lastTime) < 300000) {
//                lastPositionList.add(lastPosition);
//            }
//        }
//
//        String lastPositionJson = JSON.serialize(lastPositionList);
//
//        Jedis jedis = jedisPool.getResource();
//        Long numClients = jedis.publish("TRACKS", lastPositionJson);
//        jedisPool.returnResource(jedis);
//        long time = System.currentTimeMillis() - start;
//        log.debug("Objects published: " + lastPositionList.size() + " Clients receiving last track publishing: " + numClients + " time " + time + " ms to query and publish");
//
//    }
//
//    public static void main(String[] args) throws Exception {
//        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);
//        TrackObjectOutputter objectOutputter = new TrackObjectOutputter();
//        Map<String, Object> conf = new HashMap<String, Object>();
//        conf.put("REDIS_HOST", "localhost");
//        conf.put("REDIS_PORT", 6379);
//
//        objectOutputter.init(conf, graph);
//        int timesPublished = 0;
//        while (true) {
//            objectOutputter.publishObjects();
//            timesPublished++;
//            log.debug("Times published = " + timesPublished);
//            Thread.sleep(1000);
//        }
////        log.debug("Done publishing objects");
//    }
}
