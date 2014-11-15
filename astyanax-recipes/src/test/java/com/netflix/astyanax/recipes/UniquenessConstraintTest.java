package com.netflix.astyanax.recipes;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.CassandraTestServerProxy;

@Ignore
public class UniquenessConstraintTest {

    private static Logger LOG = LoggerFactory
            .getLogger(UniquenessConstraintTest.class);

    private static Keyspace keyspace;

    private static final String TEST_DATA_CF = "UniqueRowKeyTest";

    private static final boolean TEST_INIT_KEYSPACE = true;

    private static ColumnFamily<Long, String> CF_DATA = ColumnFamily
            .newColumnFamily(TEST_DATA_CF, LongSerializer.get(),
                    StringSerializer.get());

    @BeforeClass
    public static void setup() throws Exception {
        
        CassandraTestServerProxy.getInstance().startCassServer();
        keyspace = CassandraTestServerProxy.getInstance().getOrCreateKeyspace(ThriftFamilyFactory.getInstance());
        keyspace.createColumnFamily(CF_DATA, null);
    }

    @AfterClass
    public static void teardown() {
    }

    @Test
    public void testUniqueness() throws Exception {
        LOG.info("Starting");

        UniquenessConstraintWithPrefix<Long> unique = new UniquenessConstraintWithPrefix<Long>(
                keyspace, CF_DATA)
                .setTtl(2)
                .setPrefix("unique_")
                .setConsistencyLevel(ConsistencyLevel.CL_ONE)
                .setMonitor(
                        new UniquenessConstraintViolationMonitor<Long, String>() {
                            @Override
                            public void onViolation(Long key, String column) {
                                LOG.info("Violated: " + key + " column: "
                                        + column);
                            }
                        });

        try {
            String column = unique.isUnique(1234L);
            Assert.assertNotNull(column);
            LOG.info(column);
            column = unique.isUnique(1234L);
            Assert.assertNull(column);

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }
            column = unique.isUnique(1234L);
            Assert.assertNotNull(column);
            LOG.info(column);
        } catch (ConnectionException e) {
            LOG.error(e.getMessage());
            Assert.fail(e.getMessage());
        }
    }

}
