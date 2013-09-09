/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.tally.topology;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.script.Bindings;
import javax.script.ScriptEngineManager;
import mis.trident.blueprints.state.GroupByField;
import mis.trident.blueprints.state.SerializableMongoDBGraph;
import org.apache.log4j.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class QueryFilterer extends BaseFunction {

    private static Logger log = Logger.getLogger(QueryFilterer.class);
    
    private Long timeBin = 1L*60L*1000L;
    private SerializableMongoDBGraph graph;
    private Long lastLoadTime;
    private Map<String, Tally> tallyMap = new HashMap<String, Tally>();
    private transient GremlinGroovyScriptEngine engine;
    private Long QUERY_LOAD_INTERVAL = 1 * 60 * 1000L;

    public QueryFilterer(SerializableMongoDBGraph graph) {
        this.graph = graph;
        ScriptEngineManager factory = new ScriptEngineManager();
//        this.engine = factory.getEngineByName("groovy");
        loadQueries();
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            long timeSinceLastQueryLoad = System.currentTimeMillis() - lastLoadTime;
            if (timeSinceLastQueryLoad > QUERY_LOAD_INTERVAL) {
                loadQueries();
            }
            if (engine == null) {
//                        ScriptEngineManager factory = new ScriptEngineManager();
                this.engine = new GremlinGroovyScriptEngine();
                if (this.engine == null) {
                    log.error("Groovy engine is null and could not recreate!");
                    return;
                } else {
                    log.debug("Reinitialized groovy engine");
                }
            }
            Map<String, Object> eventOrig = (Map<String, Object>) tuple.get(0);
            Map<String,Object> event = new HashMap<String,Object>();
            event.putAll(eventOrig);
            Bindings bindings = engine.createBindings();
            for (String key : event.keySet()) {
                //log.debug("Populating binding with: "+key+" = "+event.get(key));
                bindings.put(key, event.get(key));
            }

            for (String tallyName : tallyMap.keySet()) {
                Tally tally = tallyMap.get(tallyName);
                try {

                    //query = "passedQuery = " + query;
                    //log.debug("Evaluating query: " + tally.getQuery());
//                    boolean passedQuery = false;
//                    bindings.put("passedQuery", passedQuery);
                    Object o = engine.eval(tally.getQuery(), bindings);
                    Boolean passedQuery = (Boolean)o;
//                    log.debug("Eval result: "+o.toString());
//                    log.debug("passedQuery = " + passedQuery + " after eval");
//                    passedQuery = (Boolean) bindings.get("passedQuery");
                    if (passedQuery) {
                        log.trace("Event passed " + tallyName + " event: " + event.toString());
//                        event.put("OBJECT_IDENTIFIER", tallyName);
                        Long time = (Long)event.get("TIME");
                        Long bin = time - (time % timeBin);
                        List<Object> emitValues = new ArrayList<Object>();
                        GroupByField tallyNameGroupField = new GroupByField("TALLY_NAME", tallyName);
                        emitValues.add(tallyNameGroupField);
                        emitValues.add(tally);
                        emitValues.add(event);
                        GroupByField timeBinGroupField = new GroupByField("TIME_BIN", bin);
                        emitValues.add(timeBinGroupField);
                        collector.emit(emitValues);
                    } else {
                       // log.debug("Event failed " + tallyName + " event: " + event.toString());
                    }
                } catch (Exception e) {
                    log.error("Error with groovy script", e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadQueries() {
        log.debug("Loading queries");
        GraphQuery query = this.graph.getGraph().query();
        query.has("OBJECT_TYPE", "TALLY_DEFINITION");
        Iterable<Vertex> vertices = query.vertices();
        for (Vertex v : vertices) {
            String tallyName = v.getProperty("tallyName");
            String tallyQuery = v.getProperty("tallyQuery");
            List<String> tallyFields = v.getProperty("tallyFields");
            Tally tally = new Tally(tallyName,tallyQuery,tallyFields);
            log.debug("Loaded " + tallyName + " with query: " + tallyQuery);
            this.tallyMap.put(tallyName, tally);
        }
        closeIterable(vertices);
        lastLoadTime = System.currentTimeMillis();
    }

    private void closeIterable(Iterable it) {
        if (it instanceof CloseableIterable) {
            CloseableIterable closeableIt = (CloseableIterable) it;
            closeableIt.close();
        }
    }

    public void filter() {
    }

    public static void main(String[] args) {
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        Map<String, Object> event1 = new HashMap<String, Object>();
        event1.put("TRACKID", "TRACK1");
        event1.put("PROP1", 3);

        String query1 = "PROP1 == 3";
        Bindings binding = engine.createBindings();

        for (String key : event1.keySet()) {
            binding.put(key, event1.get(key));
        }
        try {
            Object o = engine.eval(query1, binding);
            log.debug("Engine eval = " + o.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
