/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import mis.trident.blueprints.state.BlueprintsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class BlueprintsQueryProcessor extends BaseQueryFunction<BlueprintsState, List<Map<String, Object>>> {

    public List batchRetrieve(BlueprintsState state, List<TridentTuple> inputs) {
        List<List<Object>> keys = new ArrayList<List<Object>>();

        System.out.println("BatchRetrieve called on " + inputs.size() + " inputs");
        for (int i = 0; i < inputs.size(); i++) {
            TridentTuple tuple = inputs.get(i);
            ArrayList<Object> innerKey = new ArrayList<Object>();
            if (tuple == null) {
                System.out.println("BatchRetrieve null tuple!");
                continue;
            }
            System.out.println("BatchRetrieve Found tuple of size " + tuple.size());
            innerKey.add(tuple.get(0));
            for (int j = 0; j < tuple.size(); j++) {
                System.out.println("BatchRetrieve tuple value at index " + j + ": " + tuple.get(j).toString());
            }
            keys.add(innerKey);

        }

        List<Map<String, Object>> values = state.multiGet(keys);
        System.out.println("BatchRetrieve found " + values.size() + " values");
        return values;
    }

    public void execute(TridentTuple tuple, List<Map<String, Object>> objects, TridentCollector collector) {
        if (tuple != null) {
            System.out.println("BlueprintsQueryProcessor execute called on " + tuple.toString());
        } else {
            System.out.println("BlueprintsQueryProcessor execute called on null");
        }
    }
}
