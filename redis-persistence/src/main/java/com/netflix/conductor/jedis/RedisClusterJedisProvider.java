package com.netflix.conductor.jedis;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;

import javax.inject.Inject;
import javax.inject.Provider;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class RedisClusterJedisProvider implements Provider<JedisCommands> {

    private final HostSupplier hostSupplier;
    private static final int DEFAULT_TIMEOUT = 300000;
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    @Inject
    public RedisClusterJedisProvider(HostSupplier hostSupplier){
        this.hostSupplier = hostSupplier;
    }

    @Override
    public JedisCommands get() {
        // FIXME This doesn't seem very safe, but is how it was in the code this was moved from.
        // this should give redisConfigEndpoint
        // hostSupplier is created in ConfigurationHostSupplierProvider
        Host host = new ArrayList<Host>(hostSupplier.getHosts()).get(0);
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMinIdle(5);
        poolConfig.setMaxTotal(1000);
        try {
            String redisConfigEndpoint = host.getHostName();
            if(redisConfigEndpoint.contains("$")){
                redisConfigEndpoint = System.getenv(redisConfigEndpoint.replace("$","").replace("{","").replace("}",""));
            }
            System.out.println("#################################################");
            System.out.println("#################################################");
            System.out.println("REDIS_CONFIG_ENDPOINT = " + redisConfigEndpoint);
            InetAddress[] allAddress = InetAddress.getAllByName(redisConfigEndpoint);
            Set<HostAndPort> allNodes = new HashSet<HostAndPort>();
            // this will collect ip of all the nodes
            for(InetAddress address : allAddress) {
                System.out.println("Adding host node " + address.getHostAddress() + ":" + host.getPort());
                allNodes.add(new HostAndPort(address.getHostAddress(), host.getPort()));
            }
            if(!allNodes.isEmpty()) {
                System.out.println("Connecting to multiple host nodes......");
                return new JedisCluster(allNodes, poolConfig);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        System.out.println("Connecting to SINGLE host node " + host.getHostName() + ":" + host.getPort());
        return new JedisCluster(new HostAndPort(host.getHostName(), host.getPort()), poolConfig);
    }
}
