/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.tally.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import mis.trident.blueprints.state.GroupByField;
import org.apache.log4j.Logger;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TallyReducer implements ReducerAggregator<Map<String, Object>> {

    private Long callCount = 0L;
    private static Logger log = Logger.getLogger(TallyReducer.class);

    public Map<String, Object> init() {
        log.debug("Init called");
        Map<String, Object> tallyMap = new HashMap<String, Object>();
        tallyMap.put("counts", new ArrayList<Map<String, Object>>());
        return tallyMap;
    }

    public Map<String, Object> reduce(Map<String, Object> tallyCounts, TridentTuple tuple) {
        if (tuple == null) {
            log.debug("Reduce tuple is null, returning");
            return tallyCounts;
        }

        if (tallyCounts == null) {
            //log.debug("Input tally counts was null, creating new one");
            tallyCounts = new HashMap<String, Object>();
            tallyCounts.put("counts", new ArrayList<Map<String, Object>>());
        } else {
//            log.debug("Printing tally count");
//            for (String countKey : tallyCounts.keySet()) {
//                log.debug("tallyCount key = " + countKey + " value = " + tallyCounts.get(countKey).toString());
//            }
        }

//        log.debug("Tuple size: " + tuple.size());
//        for (int i = 0; i < tuple.size(); i++) {
//            log.debug("Tuple index: " + i + " value = " + tuple.get(i).toString());
//        }
        Map<String, Object> event = (Map<String, Object>) tuple.get(0);
        Tally tally = (Tally) tuple.get(1);
        GroupByField timeGroupBy = (GroupByField) tuple.get(2);
        Long tallyTimeBin = (Long) timeGroupBy.getValue();
        List<String> tallyFields = tally.getTallyFields();
        tallyCounts.put("TALLY_NAME", tally.getName());
        tallyCounts.put("TIME_BIN", tallyTimeBin);
        List<Map<String, Object>> counts = (List<Map<String, Object>>) tallyCounts.get("counts");
        if (counts == null) {
            log.debug("reinitializing count list, it was null");
            counts = new ArrayList<Map<String, Object>>();
            tallyCounts.put("counts", counts);
        }
        boolean matchedCount = false;
        for (int j = 0; j < counts.size(); j++) {
            Map<String, Object> count = counts.get(j);
            boolean eventMatchedCount = true;
            for (int i = 0; i < tallyFields.size(); i++) {
                String tallyField = tallyFields.get(i);
                if (count.get(tallyField) != null && count.get(tallyField).equals(event.get(tallyField))) {
                    //field matched, check remaining
                } else {
                    eventMatchedCount = false;
                    break;
                }
            }
            if (eventMatchedCount) {

                Integer currentCount = (Integer) count.get("count");

                Integer newCount = currentCount + 1;
                count.put("count", newCount);
                //log.debug("Count matched. "+count.toString()+" Current value: "+currentCount+" new value: "+newCount);
                matchedCount = true;
                break;
            }

        }
        if (!matchedCount) {
            Map<String, Object> newCount = new HashMap<String, Object>();
            for (int i = 0; i < tallyFields.size(); i++) {
                String tallyField = tallyFields.get(i);
                Object fieldValue = "NO VALUE";
                if (event.get(tallyField) != null) {
                    fieldValue = event.get(tallyField);
                }
                newCount.put(tallyField, fieldValue);
            }
            newCount.put("count", new Integer(1));
            //log.debug("Created new count: " + newCount.toString());
            counts.add(newCount);
        }

        callCount++;
//        log.debug("TallyReducer.reduce called: "+callCount+" times");//
        return tallyCounts;
    }
}
