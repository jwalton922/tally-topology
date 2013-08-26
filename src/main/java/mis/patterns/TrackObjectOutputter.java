/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import java.util.Map;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author jwalton
 */
public class TrackObjectOutputter extends BaseFunction{

    private JedisPoolConfig poolConfig;    
    private JedisPool jedisPool;
    
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context); 
        String redisHost = (String) conf.get("REDIS_HOST");
        Integer redisPort = Integer.parseInt(conf.get("REDIS_PORT").toString());
        poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 0);
    }

    
    
    public void execute(TridentTuple tuple, TridentCollector collector) {
        System.out.println("TrackObjectOutputter excecute called on tuple of size: "+tuple.size());
        for(int i = 0; i < tuple.size(); i++){
            if(tuple.get(i) != null){
                System.out.println("TrackObjectOutputter Tuple value at index "+i+": "+tuple.get(i).toString());
            } else {
                System.out.println("TrackObjectOutputter Tuple value at index "+i+": null");
            }
        }
    }
    
    
}
