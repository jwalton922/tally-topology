/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.tally.topology;

import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TallyOutputter extends BaseFunction {

    private static Logger log = Logger.getLogger(TallyOutputter.class);

    public void execute(TridentTuple tuple, TridentCollector collector) {
       
        List<Map<String, Object>> tallyCounts = (List) tuple.get(0);
        Long queryTime = (Long) tuple.get(1);
         log.info("TallyOutputter.execute called. Query time: "+queryTime);
        List<Map<String,Object>> filteredCounts = new ArrayList<Map<String,Object>>();
        for (int i = 0; i < tallyCounts.size(); i++) {
            Map<String, Object> tallyCount = tallyCounts.get(i);
            if (tallyCount == null) {
                log.info("null tally count!");
                continue;
            }

            Long tallyTimeBin = (Long) tallyCount.get("TIME_BIN");
            if (tallyTimeBin >= queryTime) {
                log.info(tallyTimeBin +" >= "+queryTime);
                filteredCounts.add(tallyCount);
                for (String key : tallyCount.keySet()) {
                    log.info("Tally count key: " + key + " value = " + tallyCount.get(key));
                }
            }
        }
        
        collector.emit(new Values(filteredCounts));
    }
}
