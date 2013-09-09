/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import backtype.storm.tuple.Values;
import mis.trident.blueprints.state.GroupByField;
import org.apache.log4j.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class ObjectTypeQueryProcessor extends BaseFunction{

    public static Logger log = Logger.getLogger(ObjectTypeQueryProcessor.class);
    
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String objectType = tuple.getString(0);
        log.info("ObjectTypeQueryProcessor emitting query for objects of type: "+objectType);
        
        collector.emit(new Values(new GroupByField("OBJECT_TYPE", objectType)));
        
    }
    
}
