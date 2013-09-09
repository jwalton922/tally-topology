/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import mis.trident.blueprints.state.GroupByField;
import org.json.JSONArray;
import org.json.JSONObject;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TrackEventProcessor extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        System.out.println("Tuple size: " + tuple.size());
        if (tuple.size() <= 0) {
            return;
        }
        for (int i = 0; i < tuple.size(); i++) {
            System.out.println("Tuple index: " + i + " = " + tuple.get(i).toString());
        }

        Map<String, Object> event = new HashMap<String,Object>();
        if (tuple.get(0) instanceof byte[]) {
            byte[] messageBytes = (byte[]) tuple.get(0);
            String messageString = new String(messageBytes);
            System.out.println("Event string: " + messageString);
            try {
                JSONObject json = new JSONObject(messageString);
                System.out.println("Created json object");
                Iterator keyIt = json.keys();
                while(keyIt.hasNext()){
                    String key = (String)keyIt.next();
                    Object value = json.get(key);
                    if(value instanceof JSONArray){
                        JSONArray valueArray = (JSONArray) value;
                        List<Object> valueList = new ArrayList<Object>();
                        Map<String,Object> innerMap = new HashMap<String,Object>();
                        for(int i = 0; i < valueArray.length(); i++){
                            JSONObject listObject = valueArray.getJSONObject(i);
                            Iterator innerKeyIt = listObject.keys();
                            while(innerKeyIt.hasNext()){
                                String innerKey = (String)innerKeyIt.next();
                                Object innerValue = listObject.get(innerKey);
                                innerMap.put(innerKey,innerValue);
                            }
                            valueList.add(innerMap);
                        }
                        
                        value = valueList;
                    }
                    
                    event.put(key,value);
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Unknown class type: " + tuple.get(0).getClass().getName());
        }
        String uuid = event.get("UUID").toString();
        System.out.println("Created event map");
        String trackId = event.get("FLIGHT_NUMBER").toString();
        System.out.println("trackId: "+trackId);
        GroupByField trackIdGroupByField = new GroupByField("TRACKID", trackId);
        GroupByField objectTypeGroupByField = new GroupByField("OBJECT_TYPE", "TRACK");
        collector.emit(new Values(trackIdGroupByField, objectTypeGroupByField, event, uuid));


    }
}
