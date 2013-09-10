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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Fields;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mis.patterns.ObjectTypeQueryProcessor;
import mis.patterns.TrackEventProcessor;
import mis.patterns.TrackObjectOutputter;
import mis.patterns.TrackReducer;
import mis.trackeventsimulator.BaseSensor;
import mis.trackeventsimulator.BoundingBox;
import mis.trackeventsimulator.BoundingBoxSensor;
import mis.trackeventsimulator.SimulatorState;
import mis.trackeventsimulator.TrackEventSimulatorSpout;
import mis.trident.blueprints.state.BlueprintsQueryProcessor;
import mis.trident.blueprints.state.BlueprintsState;
import mis.trident.blueprints.state.SerializableMongoDBGraph;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;

/**
 *
 * @author jwalton
 */
public class TallyTopology {

//    public StormTopology buildTopology() {
//        TrackEventBatchSpout eventSpout = null;
//        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);
//        try {
//            eventSpout = new TrackEventBatchSpout(4);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        TridentTopology topology = new TridentTopology();
//        topology.build();
//        StateFactory factory = BlueprintsState.nonTransactional(graph, Map.class);
//
//        StateFactory mongoFactory = MongoState.transactional("mongodb://127.0.0.1/test.words", Map.class);
//
//        topology.newStream("events", eventSpout).parallelismHint(1)
//                .each(new Fields("event"), new QueryFilterer(graph), new Fields("tallyName", "tally", "eventWithIdentifier", "timeBin"))
//                .groupBy(new Fields("tallyName", "timeBin")).persistentAggregate(factory, new Fields("eventWithIdentifier", "tally"), new TallyReducer(), new Fields());
//
//
//        return topology.build();
//    }

    public StormTopology buildTallyTopology(TridentTopology tridentTopology, TrackEventSimulatorSpout eventSpout, StateFactory factory, SerializableMongoDBGraph graph) {

        
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
    
    public StormTopology buildCompleteTopology(TridentTopology tridentTopology, StateFactory factory, TransactionalTridentKafkaSpout eventSpout, Graph inMemoryGraph, SerializableMongoDBGraph graph, LocalDRPC drpc){
        Stream stream = tridentTopology.newStream("events", eventSpout).parallelismHint(2);
//        TridentState tallyState = stream.each(new Fields("event"), new QueryFilterer(graph), new Fields("tallyName", "tally", "eventWithIdentifier", "timeBin"))
//                .groupBy(new Fields("tallyName", "timeBin")).persistentAggregate(factory, new Fields("eventWithIdentifier", "tally", "timeBin"), new TallyReducer(), new Fields());
//        
//        tridentTopology.newDRPCStream("tallyQuery").each(new Fields("args"), new TallyQueryProcessor(), new Fields("tallyName", "queryTime")).groupBy(new Fields("tallyName")).stateQuery(tallyState, new Fields("tallyName"), new BlueprintsQueryProcessor(), new Fields("tallyObjects")).each(new Fields("tallyObjects", "queryTime"), new TallyOutputter(), new Fields("filteredTallyObjects"));
        
        
        
        TridentState state = stream.each(eventSpout.getOutputFields(), new TrackEventProcessor(), new Fields("trackid", "objectType", "event", "eventuuid")).parallelismHint(2)
                .groupBy(new Fields("trackid", "objectType")).persistentAggregate(factory, new Fields("event", "trackid", "eventuuid"), new TrackReducer(), new Fields()).parallelismHint(1);
        
        tridentTopology.newDRPCStream("trackQuery").each(new Fields("args"), new ObjectTypeQueryProcessor(), new Fields("objectType")).groupBy(new Fields("objectType")).stateQuery(state, new Fields("objectType"), new BlueprintsQueryProcessor(), new Fields("trackObjects")).each(new Fields("trackObjects"), new TrackObjectOutputter(), new Fields("status"));                
        
        return tridentTopology.build();
    }

    public static void main(String[] args) throws Exception {
//        BasicConfigurator.configure();
//        Logger.getLogger(SimulatorState.class).
//        Logger.getLogger(BlueprintsState.class).setLevel(Level.TRACE);
//        Logger.getLogger(BlueprintsQueryProcessor.class).setLevel(Level.TRACE);
//        Logger.getLogger(TrackEventSimulatorSpout.class).setLevel(Level.TRACE);
//        Logger.getLogger(TrackObjectOutputter.class).setLevel(Level.TRACE);
//        Logger.getLogger(TallyOutputter.class).setLevel(Level.TRACE);
//        Logger.getLogger(TallyQueryProcessor.class).setLevel(Level.TRACE);
        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);
        Graph inMemoryGraph = new TinkerGraph();
        
        StateFactory factory = new BlueprintsState.Factory<Map>(inMemoryGraph, StateType.NON_TRANSACTIONAL, Map.class, new BlueprintsState.Options());
//        StateFactory factory = new MemoryMapState.Factory();
        SimulatorState simState = new SimulatorState(10, System.currentTimeMillis(), 100.0);
        simState.setTakeoffChance(0.5);
        List<BaseSensor> sensors = new ArrayList<BaseSensor>();
        sensors.add(new BoundingBoxSensor("WESTERN_HEMISPHERE_FEED", "NORTH_SENSOR", 1.0, new BoundingBox(-180, 0, 0, 90)));
        sensors.add(new BoundingBoxSensor("WESTERN_HEMISPHERE_FEED", "SOUTH_SENSOR", 1.0, new BoundingBox(-180, -90, 0, 0)));
        sensors.add(new BoundingBoxSensor("EASTERN_HEMISPHERE_FEED", "NORTH_SENSOR", 1.0, new BoundingBox(0, 0, 180, 90)));
        sensors.add(new BoundingBoxSensor("EASTERN_HEMISPHERE_FEED", "SOUTH_SENSOR", 1.0, new BoundingBox(0, -90, 180, 0)));
//        TrackEventSimulatorSpout eventSpout = new TrackEventSimulatorSpout(simState, sensors);
        List<String> hosts = new ArrayList<String>();
        hosts.add("127.0.0.1");
//         SpoutConfig spoutConfig = new SpoutConfig(
//                KafkaConfig.StaticHosts.fromHostString(hosts, 1),
//                "test3",
//                "/kafkastorm",
//                "discovery");
         
//         KafkaSpout eventSpout = new KafkaSpout(spoutConfig);
//         System.out.println("output fields: "+spoutConfig.scheme.getOutputFields().toList().get(0));
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(KafkaConfig.StaticHosts.fromHostString(hosts, 1), "tracks2");
        
         TransactionalTridentKafkaSpout eventSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
//        KafkaSpout eventSpout = new KafkaSpout(spoutConfig);
        TridentTopology topology = new TridentTopology();
        topology.build();     
        TallyTopology tt = new TallyTopology();
        
        LocalDRPC drpc = new LocalDRPC();
//        tt.buildTallyTopology(topology,eventSpout, factory, graph);
//        TridentState patternState = tt.buildPatternToplogy(topology,eventSpout, factory);
//        tt.buildTrackQueryToplogy(topology, patternState, drpc);
        
        StormTopology completeToplogy = tt.buildCompleteTopology(topology, factory, eventSpout, inMemoryGraph, graph, drpc);
        
        Config conf = new Config();
        conf.put("REDIS_HOST", "localhost");
        conf.put("REDIS_PORT", new Integer(6379));
        conf.setMaxSpoutPending(10);
        if (args.length == 0) {
            System.out.println("Running in local mode!");
            LocalCluster cluster = new LocalCluster();
            //cluster.submitTopology("tally", conf, tallyTopology);
            cluster.submitTopology("patterns", conf, completeToplogy);
            while(true) {
                long startDate = System.nanoTime();
                System.out.println("trackQuery");
                String result = drpc.execute("trackQuery", "TRACK");
//                String result = drpc.execute("tallyQuery", "SIM_TALLY 1377888059");
                long endDate = System.nanoTime() - startDate;
                System.out.println("DRPC RESULT: " + result + " took: " + endDate / 1000000);
                Thread.sleep(100);
            }
//            cluster.shutdown();
        } else {
            System.out.println("Submitting tally topology to storm cluster");
            conf.setNumWorkers(3);
           // StormSubmitter.submitTopology(args[0]+"TALLY", conf, tallyTopology);
            StormSubmitter.submitTopology(args[0]+"PATTERN", conf, completeToplogy);
//            while(true) {
//                long startDate = System.nanoTime();
//                System.out.println("trackQuery");
//                String result = drpc.execute("trackQuery", "TRACK");
////                String result = drpc.execute("tallyQuery", "SIM_TALLY 1377888059");
//                long endDate = System.nanoTime() - startDate;
//                System.out.println("DRPC RESULT: " + result + " took: " + endDate / 1000000);
//                Thread.sleep(100);
//            }
        }
    }
}
