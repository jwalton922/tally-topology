/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import mis.trident.blueprints.state.GroupByField;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TrackReducer implements ReducerAggregator<Map<String, Object>>{

    public Map<String, Object> init() {
        return new HashMap<String,Object>();
    }

    public Map<String, Object> reduce(Map<String, Object> trackObject, TridentTuple tuple) {
        if(trackObject == null){
            System.out.println("Input track object is null");
            trackObject = new HashMap<String,Object>();
        }
        
        System.out.println("Tuple size: "+tuple.size());
        for(int i = 0; i < tuple.size(); i++){
            System.out.println("Tuple index "+i+": "+tuple.get(i).toString());
        }
        Map<String,Object> event = (Map<String,Object>)tuple.get(0);
        GroupByField trackId = (GroupByField) tuple.get(1);
        
        List<Map<String,Object>> positions =  (List<Map<String,Object>>) trackObject.get("POSITIONS");
        if(positions == null){
            positions = new ArrayList<Map<String,Object>>();
            trackObject.put("POSITIONS", positions);
        }
        
        Long time = (Long) event.get("TIME");
        Double lat = (Double) event.get("LATITUDE");
        Double lon = (Double) event.get("LONGITUDE");
        Map<String,Object> position = new HashMap<String,Object>();
//        position.put("TIME", time);
//        position.put("LATITUDE", lat);
//        position.put("LONGITUDE", lon);
        position.putAll(event);
        positions.add(position);
        trackObject.putAll(event);
        
        
        return trackObject;
    }
    
}
