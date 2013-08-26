/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import backtype.storm.tuple.Values;
import java.util.Map;
import mis.trident.blueprints.state.GroupByField;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TrackEventProcessor  extends BaseFunction{

    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.get(0);
        String trackId = event.get("TRACKID").toString();
        GroupByField trackIdGroupByField = new GroupByField("TRACKID", trackId);
        GroupByField objectTypeGroupByField = new GroupByField("OBJECT_TYPE", "TRACK");
        collector.emit(new Values(trackIdGroupByField, objectTypeGroupByField));
        
        
    }
    
    
}
