package com.netflix.astyanax.thrift;

import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.util.CassandraTestServerProxy;

public class ThriftClusterImplTest {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClusterImplTest.class);
    
    private static Cluster cluster;
    
    @BeforeClass
    public static void setup() throws Exception {
        System.out.println("TESTING THRIFT KEYSPACE");
        
        CassandraTestServerProxy.getInstance().startCassServer();
        
        cluster = CassandraTestServerProxy.getInstance().getOrCreateCluster(ThriftFamilyFactory.getInstance());
    }

    @AfterClass
    public static void teardown() throws Exception {
    }

    @Test
    public void test() throws Exception {
        
        String keyspaceName = "ClusterTest";
        
        try {
            cluster.dropKeyspace(keyspaceName);
            if (CassandraTestServerProxy.getInstance().useRemoteCassandra()) {
                Thread.sleep(10*1000);
            }
        } catch (Exception e) {
        }
        
        Map<String, Object> options = null;
        
        if (CassandraTestServerProxy.getInstance().useRemoteCassandra()) {
            options = ImmutableMap.<String, Object>builder()
                    .put("name", keyspaceName)
                    .put("strategy_options", ImmutableMap.<String, Object>builder()
                            .put("us-east", "3")
                            .put("eu-west", "3")
                            .build())
                    .put("strategy_class",     "NetworkTopologyStrategy").build();
        } else {
            
            options = ImmutableMap.<String, Object>builder()
                    .put("name", keyspaceName)
                    .put("strategy_options", ImmutableMap.<String, Object>builder()
                            .put("replication_factor", "1")
                            .build())
                            .put("strategy_class",     "SimpleStrategy")
                            .build();
        }
        
        cluster.createKeyspace(options);
        
        Properties prop1 = cluster.getKeyspaceProperties(keyspaceName);
        System.out.println(prop1);
        Assert.assertTrue(prop1.containsKey("name"));
        Assert.assertTrue(prop1.containsKey("strategy_class"));
        
        Properties prop2 = cluster.getAllKeyspaceProperties();
        System.out.println(prop2);
        Assert.assertTrue(prop2.containsKey("ClusterTest.name"));
        Assert.assertTrue(prop2.containsKey("ClusterTest.strategy_class"));
        
        Properties cfProps = new Properties();
        cfProps.put("keyspace",   keyspaceName);
        cfProps.put("name",       "cf1");
        cfProps.put("compression_options.sstable_compression", "");
        
        cluster.createColumnFamily(cfProps);
        
        Properties cfProps1 = cluster.getKeyspaceProperties(keyspaceName);
        KeyspaceDefinition ksdef = cluster.describeKeyspace(keyspaceName);
        ColumnFamilyDefinition cfdef = ksdef.getColumnFamily("cf1");
        LOG.info(cfProps1.toString());
        
        LOG.info(cfdef.getProperties().toString());
        Assert.assertEquals(cfProps1.get("cf_defs.cf1.comparator_type"), "org.apache.cassandra.db.marshal.BytesType");
    }
}
