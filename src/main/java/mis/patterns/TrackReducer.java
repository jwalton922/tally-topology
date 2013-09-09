/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import mis.trident.blueprints.state.GroupByField;
import org.apache.log4j.Logger;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TrackReducer implements ReducerAggregator<Map<String, Object>> {

    private static Logger log = Logger.getLogger(TrackReducer.class);
    private long maxTimeDiff = 1000 * 60 * 20;
    public Map<String, Object> init() {
        return new HashMap<String, Object>();
    }
    
    private Set<String> uuids = new HashSet<String>();

    public Map<String, Object> reduce(Map<String, Object> trackObject, TridentTuple tuple) {
        long start = System.currentTimeMillis();
        if (trackObject == null) {
            System.out.println("Input track object is null");
            trackObject = new HashMap<String, Object>();
        }

//        System.out.println("Tuple size: "+tuple.size());
//        for(int i = 0; i < tuple.size(); i++){
//            System.out.println("Tuple index "+i+": "+tuple.get(i).toString());
//        }
        Map<String, Object> event = (Map<String, Object>) tuple.get(0);
        GroupByField trackId = (GroupByField) tuple.get(1);
        String uuid = (String) tuple.get(2);
        List<Map<String, Object>> positions = (List<Map<String, Object>>) trackObject.get("POSITIONS");
        if (positions == null) {
            positions = new ArrayList<Map<String, Object>>();
            trackObject.put("POSITIONS", positions);
        }

        Long time = (Long) event.get("TIME");
        long current = System.currentTimeMillis();
        long diff = (current - time);
        if (diff > maxTimeDiff) {
            log.info("Not storing position it is: " + diff + " ms old");
        } else {
//        Double lat = Double.parseDouble(event.get("LATITUDE").toString());
//        Double lon = Double.parseDouble(event.get("LONGITUDE").toString());
            Map<String, Object> position = new HashMap<String, Object>();
//        position.put("TIME", time);
//        position.put("LATITUDE", lat);
//        position.put("LONGITUDE", lon);
            position.putAll(event);
            positions.add(position);
            trackObject.putAll(event);
            log.info("Track: "+trackId+" now has "+positions.size()+" positions");

        }
        long reduceTime = System.currentTimeMillis() - start;
        log.info("It took "+reduceTime+" ms to process event "+uuid+" reduce step finished: "+System.nanoTime());
        return trackObject;
    }
}
