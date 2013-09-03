/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.tally.topology;

import backtype.storm.tuple.Values;
import mis.patterns.ObjectTypeQueryProcessor;
import static mis.patterns.ObjectTypeQueryProcessor.log;
import mis.trident.blueprints.state.GroupByField;
import org.apache.log4j.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TallyQueryProcessor extends BaseFunction{
    
    public static Logger log = Logger.getLogger(ObjectTypeQueryProcessor.class);
    
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String tallyArgs = tuple.getString(0);
        String[] argsSplit = tallyArgs.split("\\s+");
        String tallyName = argsSplit[0];
        Long oldestTime = Long.parseLong(argsSplit[1]);
        
        log.debug("TallyQueryProcessor emitting query for TALLY_NAME: "+tallyName);
        
        collector.emit(new Values(new GroupByField("TALLY_NAME", tallyName), oldestTime));
        
    }
}
