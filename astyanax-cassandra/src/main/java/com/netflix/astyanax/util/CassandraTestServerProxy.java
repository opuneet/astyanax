package com.netflix.astyanax.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.thrift.Cassandra;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxTypeFactory;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;

public class CassandraTestServerProxy {

    public static String TEST_CLUSTER_NAME  = "cass_sandbox";
    //public static String TEST_KEYSPACE_NAME = "AstyanaxUnitTests";
    public static String TEST_KEYSPACE_NAME = "astyanaxunittests";

    private static final String LOCAL_SEEDS = "localhost:9160";
    
    //public static final String REMOTE_SEEDS = "ec2-54-235-19-77.compute-1.amazonaws.com:7102";
    //public static final String REMOTE_SEEDS = "ec2-54-163-99-45.compute-1.amazonaws.com:7102";
    public static final String REMOTE_SEEDS = null;
    private static boolean RecreateKeyspaceOverride = true; 
    
    private static final CassandraTestServerProxy Instance = new CassandraTestServerProxy();

    public static CassandraTestServerProxy getInstance() {
        return Instance;
    }
    
    private final ConcurrentHashMap<String, AstyanaxContext<Keyspace>> ksContextMap = new ConcurrentHashMap<String, AstyanaxContext<Keyspace>>();
    private final ConcurrentHashMap<String, AstyanaxContext<Cluster>> clContextMap = new ConcurrentHashMap<String, AstyanaxContext<Cluster>>();
    
    private CassandraTestServerProxy() {
        
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                super.run();
                System.out.println("INSIDE SHUTDOWN HOOK. Stopping all Astyanax conn pools");
                
                try {
                    Thread.sleep(5*1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                
                for (AstyanaxContext<Keyspace> ksContext : ksContextMap.values()) {
                    try { 
                        ksContext.shutdown();
                    } catch (Exception e) {
                        // do nothing
                    }
                }
                
                for (AstyanaxContext<Cluster> clContext : clContextMap.values()) {
                    try { 
                        clContext.shutdown();
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            }
            
        });
    }
    
    public Keyspace getOrCreateKeyspace(AstyanaxTypeFactory<Cassandra.Client> typeFactory) throws Exception {
        return getOrCreateKeyspace(TEST_CLUSTER_NAME, TEST_KEYSPACE_NAME, typeFactory, true);
    }
    
    public Cluster getOrCreateCluster(AstyanaxTypeFactory<Cassandra.Client> typeFactory) {
        return getOrCreateCluster(TEST_CLUSTER_NAME, typeFactory);
    }

    public Cluster getOrCreateCluster(String cluster, AstyanaxTypeFactory<Cassandra.Client> typeFactory) {
        
        AstyanaxContext<Cluster> clCtx = clContextMap.get(cluster);
        
        if (clCtx == null) {
            synchronized (this) {
                
                clCtx = clContextMap.get(cluster);
                if (clCtx != null) {
                    return clCtx.getClient();
                }

                clCtx = new AstyanaxContext.Builder()
                .forCluster(cluster)
                .forKeyspace(TEST_KEYSPACE_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.DISCOVERY_SERVICE)
                                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                                .setDiscoveryDelayInSeconds(60000)
                                )
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME)
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(20)
                                .setInitConnsPerHost(10)
                                //.setSeeds(getSeedHost())
                                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .withHostSupplier(new Supplier<List<Host>>() {

                    @Override
                    public List<Host> get() {
                        final List<Host> list = new ArrayList<Host>();
                        list.add(new Host(getSeedHost(), getSeedPort()));
                        return list;
                    }
                })
                .buildCluster(typeFactory);
        
                clCtx.start();
                clContextMap.put(cluster, clCtx);
                
                return clCtx.getClient();
            }
        } else {
            return clCtx.getClient();
        }
    }
    
    public Keyspace getOrCreateKeyspace(String cluster, String keyspace, AstyanaxTypeFactory<Cassandra.Client> typeFactory, boolean recreateKeysapce) throws Exception {
        
        String key = getKey(cluster, keyspace);
        AstyanaxContext<Keyspace> ksCtx = ksContextMap.get(key);
        
        if (ksCtx == null) {
            synchronized (this) {
                
                ksCtx = ksContextMap.get(key);
                if (ksCtx != null) {
                    return ksCtx.getClient();
                }

                ksCtx = new AstyanaxContext.Builder()
                .forCluster(cluster)
                .forKeyspace(keyspace)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setCqlVersion("3.0.0")
                                .setTargetCassandraVersion("1.2")
                                .setDiscoveryType(NodeDiscoveryType.DISCOVERY_SERVICE)
                                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                                .setDiscoveryDelayInSeconds(60000))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(TEST_CLUSTER_NAME
                                + "_" + TEST_KEYSPACE_NAME)
                                .setSocketTimeout(30000)
                                .setMaxTimeoutWhenExhausted(2000)
                                .setMaxConnsPerHost(20)
                                .setInitConnsPerHost(10)
                                //.setSeeds(getSeedHost())
                                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .withHostSupplier(new Supplier<List<Host>>() {

                    @Override
                    public List<Host> get() {
                        final List<Host> list = new ArrayList<Host>();
                        list.add(new Host(getSeedHost(), getSeedPort()));
                        return list;
                    }
                })
                .buildKeyspace(typeFactory);
                
                ksCtx.start();
                ksContextMap.put(key, ksCtx);
                
                if (recreateKeysapce && RecreateKeyspaceOverride) {
                   dropAndRecreateKeyspace(ksCtx.getClient());
                }
                
                return ksCtx.getClient();
            }
        } else {
            return ksCtx.getClient();
        }
    }
    
    private void dropAndRecreateKeyspace(Keyspace keyspace) throws Exception {
        
        System.out.println("DROPPING AND RECREATING KEYSPACE: " + keyspace.getKeyspaceName());
        try {
            keyspace.dropKeyspace();
        } catch (Exception e) {
        }
       createKeyspace(keyspace);
       
    }
    
    public void createKeyspace(Keyspace keyspace) throws Exception {
        if (useRemoteCassandra()) {
            createKeyspaceRemote(keyspace);
        } else {
            createKeyspaceLocal(keyspace);
        }
    }
    
    private void createKeyspaceLocal(Keyspace keyspace) throws Exception {
        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                        .put("strategy_class",     "SimpleStrategy")
                        .build()
                );
    }
    
    private void createKeyspaceRemote(Keyspace keyspace) throws Exception {
        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("us-east", "3")
                        .put("eu-west", "3")
                        .build())
                .put("strategy_class",     "NetworkTopologyStrategy").build()
                );
    }
    
    public void startCassServer() {
        if (useRemoteCassandra()) {
            System.out.println("USING REMOTE CASSANDRA SERVER");
            return; // do nothing. 
        } else {
            System.out.println("USING LOCAL EMBEDDED CASSANDRA SERVER");
            SingletonEmbeddedCassandra.getInstance();
        }
    }
    
    private String getKey(String cluster, String keyspace) {
        return cluster + "." + keyspace;
    }

    public String getSeedHost() {
        
        String seedHost = null; 
        
        // Check local override
        seedHost = (REMOTE_SEEDS != null && !REMOTE_SEEDS.isEmpty()) ? REMOTE_SEEDS : null; 
        
        // Check java runtime property
        if (seedHost == null || seedHost.isEmpty()) {
            String seedHostFromRuntimeProperty = System.getProperty("astyanax.unitTest.seedHost");
            seedHost = (seedHostFromRuntimeProperty != null && !seedHostFromRuntimeProperty.isEmpty()) ? seedHostFromRuntimeProperty : null; 
        }
        
        // If still null then use LOCAL_SEEDS
        if (seedHost == null || seedHost.isEmpty()) {
            seedHost = LOCAL_SEEDS;
        }
        return seedHost;
    }
    
    public boolean useRemoteCassandra() {
        String seedHost = getSeedHost();
        return !seedHost.equals(LOCAL_SEEDS);
    }
    
    private int getSeedPort() {
        return useRemoteCassandra() ? 7102 : 9160; 
    }
    
    
  
}
