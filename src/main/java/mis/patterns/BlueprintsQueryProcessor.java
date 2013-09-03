/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;


import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import mis.trident.blueprints.state.BlueprintsState;
import org.apache.log4j.Logger;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class BlueprintsQueryProcessor extends BaseQueryFunction<BlueprintsState, Object> {

    public static Logger log = Logger.getLogger(BlueprintsQueryProcessor.class);
    
    public List batchRetrieve(BlueprintsState state, List<TridentTuple> inputs) {
        List<List<Object>> keys = new ArrayList<List<Object>>();

       // log.debug("BatchRetrieve called on " + inputs.size() + " inputs");
        for (int i = 0; i < inputs.size(); i++) {
            TridentTuple tuple = inputs.get(i);
            ArrayList<Object> innerKey = new ArrayList<Object>();
            if (tuple == null) {
                log.debug("BatchRetrieve null tuple!");
                continue;
            }
            //log.debug("BatchRetrieve Found tuple of size " + tuple.size());
            innerKey.add(tuple.get(0));
            for (int j = 0; j < tuple.size(); j++) {
                log.debug("BatchRetrieve tuple value at index " + j + ": " + tuple.get(j).toString());
            }
            keys.add(innerKey);

        }
        List returnList = new ArrayList();
        List values = state.multiGet(keys);
//        for(Object value : values){
//            if(value == null){
//                log.debug("BatchRetrieve value: null");
//                continue;
//            }
//            //log.debug("BatchRetrieve value: "+value.toString());
//        }
       // log.debug("BatchRetrieve found " + values.size() + " values");
        returnList.add(values);
        return returnList;
    }

    public void execute(TridentTuple tuple, Object object, TridentCollector collector) {
        if (tuple != null) {
            //log.debug("BlueprintsQueryProcessor execute called on tuple: "+ tuple.toString());
        } else {
            log.debug("BlueprintsQueryProcessor execute called on null tuple");
        }
        
        if(object != null){
           // log.debug("BlueprintsQueryProcessor execute called on object: "+ object.toString());
        } else {
            log.debug("BlueprintsQueryProcessor execute called on null OBJECT");
            return;
        }
        collector.emit(new Values(object));
//        if(object instanceof List){
//            List values = (List) object;
//            for(int i = 0; i < values.size(); i++){
//                Object innerValue = values.get(i);
//                if(innerValue == null){
//                    log.debug("innerValue is null!");
//                    continue;
//                }
//                collector.emit(new Values(innerValue));
////                if(innerValue instanceof List){
////                    List innerList = (List) innerValue;
////                    for(int j = 0; j < innerList.size(); j++){
////                        collector.emit(new Values(innerList.get(j)));
////                    }
////                } else {
////                    log.debug("Unknown innerValue type: "+innerValue.getClass().getName());
////                }
//            }
//        }
        
    }
}
