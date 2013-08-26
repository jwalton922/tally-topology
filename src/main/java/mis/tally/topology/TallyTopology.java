/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.tally.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.sjoerdmulder.trident.mongodb.MongoState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import mis.patterns.BlueprintsQueryProcessor;
import mis.patterns.ObjectTypeQueryProcessor;
import mis.patterns.TrackEventProcessor;
import mis.patterns.TrackObjectOutputter;
import mis.patterns.TrackReducer;
import mis.trackeventsimulator.BaseSensor;
import mis.trackeventsimulator.BoundingBox;
import mis.trackeventsimulator.BoundingBoxSensor;
import mis.trackeventsimulator.SimulatorState;
import mis.trackeventsimulator.TrackEventSimulatorSpout;
import mis.trident.blueprints.state.BlueprintsState;
import mis.trident.blueprints.state.GroupByField;
import mis.trident.blueprints.state.SerializableMongoDBGraph;
import mis.trident.blueprints.state.TrackEventBatchSpout;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;

/**
 *
 * @author jwalton
 */
public class TallyTopology {

    public StormTopology buildTopology() {
        TrackEventBatchSpout eventSpout = null;
        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);
        try {
            eventSpout = new TrackEventBatchSpout(4);
        } catch (Exception e) {
            e.printStackTrace();
        }

        TridentTopology topology = new TridentTopology();
        topology.build();
        StateFactory factory = BlueprintsState.nonTransactional(graph, Map.class);

        StateFactory mongoFactory = MongoState.transactional("mongodb://127.0.0.1/test.words", Map.class);

        topology.newStream("events", eventSpout).parallelismHint(1)
                .each(new Fields("event"), new QueryFilterer(graph), new Fields("tallyName", "tally", "eventWithIdentifier", "timeBin"))
                .groupBy(new Fields("tallyName", "timeBin")).persistentAggregate(factory, new Fields("eventWithIdentifier", "tally"), new TallyReducer(), new Fields());


        return topology.build();
    }

    public StormTopology buildSimTopology(TridentTopology tridentTopology, TrackEventSimulatorSpout eventSpout, StateFactory factory, SerializableMongoDBGraph graph) {

        
        tridentTopology.newStream("events", eventSpout).parallelismHint(1)
                .each(new Fields("event"), new QueryFilterer(graph), new Fields("tallyName", "tally", "eventWithIdentifier", "timeBin"))
                .groupBy(new Fields("tallyName", "timeBin")).persistentAggregate(factory, new Fields("eventWithIdentifier", "tally"), new TallyReducer(), new Fields());
        
        return tridentTopology.build();
    }
    
    public TridentState buildPatternToplogy(TridentTopology tridentTopology, TrackEventSimulatorSpout eventSpout, StateFactory factory){

        TridentState patternState = tridentTopology.newStream("patterns", eventSpout).parallelismHint(1)
                .each(new Fields("event"), new TrackEventProcessor(), new Fields("trackid", "objectType"))
                .groupBy(new Fields("trackid", "objectType")).persistentAggregate(factory, new Fields("event", "trackid"), new TrackReducer(), new Fields());
        return patternState;
    }
    
    public StormTopology buildTrackQueryToplogy(TridentTopology tridentToplogy, TridentState state, LocalDRPC drpc){
        
        //query stream
        tridentToplogy.newDRPCStream("trackQuery", drpc).each(new Fields("args"), new ObjectTypeQueryProcessor(), new Fields("objectType")).groupBy(new Fields("objectType")).stateQuery(state, new Fields("objectType"), new MapGet(), new Fields("trackObjects")).each(new Fields("trackObjects"), new TrackObjectOutputter(), new Fields());
        return tridentToplogy.build();
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getLogger(SimulatorState.class).setLevel(Level.TRACE);
        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);
        
        
        StateFactory factory = BlueprintsState.nonTransactional(graph, Map.class);
        SimulatorState simState = new SimulatorState(10000, System.currentTimeMillis());
        List<BaseSensor> sensors = new ArrayList<BaseSensor>();
        sensors.add(new BoundingBoxSensor("WESTERN_HEMISPHERE_FEED", "NORTH_SENSOR", 1.0, new BoundingBox(-180, 0, 0, 90)));
        sensors.add(new BoundingBoxSensor("WESTERN_HEMISPHERE_FEED", "SOUTH_SENSOR", 1.0, new BoundingBox(-180, -90, 0, 0)));
        sensors.add(new BoundingBoxSensor("EASTERN_HEMISPHERE_FEED", "NORTH_SENSOR", 1.0, new BoundingBox(0, 0, 180, 90)));
        sensors.add(new BoundingBoxSensor("EASTERN_HEMISPHERE_FEED", "SOUTH_SENSOR", 1.0, new BoundingBox(0, -90, 180, 0)));
        TrackEventSimulatorSpout eventSpout = new TrackEventSimulatorSpout(simState, sensors);
        TridentTopology topology = new TridentTopology();
        topology.build();     
        TallyTopology tt = new TallyTopology();
        LocalDRPC drpc = new LocalDRPC();
        tt.buildSimTopology(topology,eventSpout, factory, graph);
        TridentState patternState = tt.buildPatternToplogy(topology,eventSpout, factory);
        tt.buildTrackQueryToplogy(topology, patternState, drpc);
        
        StormTopology completeToplogy = topology.build();
        
        Config conf = new Config();
        conf.put("REDIS_HOST", "localhost");
        conf.put("REDIS_PORT", new Integer(6379));
        conf.setMaxSpoutPending(5);
        if (args.length == 0) {
            
            LocalCluster cluster = new LocalCluster();
            //cluster.submitTopology("tally", conf, tallyTopology);
            cluster.submitTopology("patterns", conf, completeToplogy);
            for (int i = 0; i < 5; i++) {
                long startDate = System.nanoTime();
                drpc.execute("trackQuery", "TRACK");
//                long endDate = System.nanoTime() - startDate;
//                System.out.println("DRPC RESULT: " + result + " took: " + endDate / 1000000);
                Thread.sleep(100);
            }
//            cluster.shutdown();
        } else {
            conf.setNumWorkers(3);
           // StormSubmitter.submitTopology(args[0]+"TALLY", conf, tallyTopology);
            StormSubmitter.submitTopology(args[0]+"PATTERN", conf, completeToplogy);
        }
    }
}
