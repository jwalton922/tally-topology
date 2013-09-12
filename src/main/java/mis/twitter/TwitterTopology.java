/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.twitter;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import mis.patterns.ObjectTypeQueryProcessor;
import mis.patterns.TrackObjectOutputter;
import mis.trident.blueprints.state.BlueprintsQueryProcessor;
import mis.trident.blueprints.state.BlueprintsState;
import mis.trident.blueprints.state.SerializableMongoDBGraph;
import storm.kafka.KafkaConfig;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;

/**
 *
 * @author jwalton
 */
public class TwitterTopology {
    public StormTopology buildCompleteTopology(TridentTopology tridentTopology, StateFactory factory, TransactionalTridentKafkaSpout eventSpout, Graph inMemoryGraph, SerializableMongoDBGraph graph, LocalDRPC drpc){
        Stream stream = tridentTopology.newStream("events", eventSpout).parallelismHint(2);
//        TridentState tallyState = stream.each(new Fields("event"), new QueryFilterer(graph), new Fields("tallyName", "tally", "eventWithIdentifier", "timeBin"))
//                .groupBy(new Fields("tallyName", "timeBin")).persistentAggregate(factory, new Fields("eventWithIdentifier", "tally", "timeBin"), new TallyReducer(), new Fields());
//        
//        tridentTopology.newDRPCStream("tallyQuery").each(new Fields("args"), new TallyQueryProcessor(), new Fields("tallyName", "queryTime")).groupBy(new Fields("tallyName")).stateQuery(tallyState, new Fields("tallyName"), new BlueprintsQueryProcessor(), new Fields("tallyObjects")).each(new Fields("tallyObjects", "queryTime"), new TallyOutputter(), new Fields("filteredTallyObjects"));
        
        
        
        TridentState tweetState = stream.each(eventSpout.getOutputFields(), new TweetProcessor(), new Fields("userId", "objectType","tweet")).parallelismHint(3).groupBy(new Fields("userId", "objectType")).persistentAggregate(factory, new Fields("userId", "tweet"), new TweetReducer(), new Fields());
//                .groupBy(new Fields("trackid", "objectType")).persistentAggregate(factory, new Fields("event", "trackid", "eventuuid"), new TrackReducer(), new Fields()).parallelismHint(1);
        
        tridentTopology.newDRPCStream("twitterQuery").each(new Fields("args"), new ObjectTypeQueryProcessor(), new Fields("objectType")).groupBy(new Fields("objectType")).stateQuery(tweetState, new Fields("objectType"), new BlueprintsQueryProcessor(), new Fields("twitterUsers")).each(new Fields("twitterUsers"), new TwitterUserPersister(), new Fields());                
        
        return tridentTopology.build();
    }
    
    public static void main(String[] args) throws Exception {
        SerializableMongoDBGraph graph = new SerializableMongoDBGraph("localhost", 27017);
        Graph inMemoryGraph = new TinkerGraph();
        
        StateFactory factory = new BlueprintsState.Factory<Map>(inMemoryGraph, StateType.NON_TRANSACTIONAL, Map.class, new BlueprintsState.Options());

        List<String> hosts = new ArrayList<String>();
        hosts.add("127.0.0.1");

        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(KafkaConfig.StaticHosts.fromHostString(hosts, 1), "flightDelay");
        
         TransactionalTridentKafkaSpout eventSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
        TridentTopology topology = new TridentTopology();
        topology.build();     
        TwitterTopology tt = new TwitterTopology();
        
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
//            System.out.println("Running in local mode!");
//            LocalCluster cluster = new LocalCluster();
//
//            cluster.submitTopology("patterns", conf, completeToplogy);
//            while(true) {
//                long startDate = System.nanoTime();
//                System.out.println("trackQuery");
//                String result = drpc.execute("trackQuery", "TRACK");
//                long endDate = System.nanoTime() - startDate;
//                System.out.println("DRPC RESULT: " + result + " took: " + endDate / 1000000);
//                Thread.sleep(100);
//            }
        } else {
            System.out.println("Submitting tally topology to storm cluster");
            conf.setNumWorkers(3);
           // StormSubmitter.submitTopology(args[0]+"TALLY", conf, tallyTopology);
            StormSubmitter.submitTopology(args[0]+"Twitter", conf, completeToplogy);

        }
    }
}
