/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mis.patterns;

import backtype.storm.utils.DRPCClient;
import com.mongodb.util.JSON;
import java.util.List;


import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 * @author jwalton
 */
public class TrackDrpcQueryThread {

    private JedisPoolConfig poolConfig;
    private JedisPool jedisPool;
    DRPCClient client = new DRPCClient("localhost", 3772);

    public TrackDrpcQueryThread(String redisHost, int redisPort) {
        this.init(redisHost, redisPort);
    }

    public void init(String redisHost, int redisPort) {
        poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 0);
    }

    public void performDrpcQuery() {
        try {
            System.out.println("Performing DRPC TRACK query");
            String result = client.execute("trackQuery", "TRACK");
            System.out.println("result: "+result+" //NOTE outputter writes to redis!");
            /**
             * The trackQuery topology writes the last positions to a pub/sub REDIS service
             */
//            System.out.println("result: "+result);
//            List resultObject = (List)JSON.parse(result);
//            for(int i = 0; i < resultObject.size(); i++){
//                System.out.println("Object at index "+i+": "+resultObject.get(i).toString());
//            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{

        TrackDrpcQueryThread queryThread = new TrackDrpcQueryThread("localhost", 6379);
        while (true) {
            queryThread.performDrpcQuery();
            Thread.sleep(1000);
        }
    }
}
