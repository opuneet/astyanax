package com.netflix.astyanax.thrift;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.cql.CqlSchema;
import com.netflix.astyanax.cql.CqlStatementResult;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.MapSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.astyanax.util.CassandraTestServerProxy;

public class CqlTest {

    private static Logger Log = LoggerFactory.getLogger(CqlTest.class);

    private static Keyspace keyspace;

    private static final long CASSANDRA_WAIT_TIME = 1000;
    static ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    static ColumnFamily<String, String> User_CF = ColumnFamily.newColumnFamily(
            "UserCF", StringSerializer.get(), StringSerializer.get());
    static ColumnFamily<UUID, String> UUID_CF = ColumnFamily.newColumnFamily(
            "uuidtest", UUIDSerializer.get(), StringSerializer.get());

    @BeforeClass
    public static void setup() throws Exception {
        CassandraTestServerProxy.getInstance().startCassServer();
        createKeyspace();
    }

    @AfterClass
    public static void teardown() throws Exception {
    }

    public static void createKeyspace() throws Exception {
        
        keyspace = CassandraTestServerProxy.getInstance().getOrCreateKeyspace(ThriftFamilyFactory.getInstance());

        OperationResult<CqlStatementResult> result;

        result = keyspace
                .prepareCqlStatement()
                .withCql(
                        "CREATE TABLE employees (empID int, deptID int, first_name varchar, last_name varchar, PRIMARY KEY (empID, deptID));")
                .execute();

        result = keyspace
                .prepareCqlStatement()
                .withCql(
                        "CREATE TABLE users (id text PRIMARY KEY, given text, surname text, favs map<text, text>);")
                .execute();

        Thread.sleep(CASSANDRA_WAIT_TIME);

        KeyspaceDefinition ki = keyspace.describeKeyspace();
        Log.info("Describe Keyspace: " + ki.getName());
    }

    @Test
    public void testCompoundKey() throws Exception {
        OperationResult<CqlStatementResult> result;
        result = keyspace
                .prepareCqlStatement()
                .withCql(
                        "INSERT INTO employees (empID, deptID, first_name, last_name) VALUES (111, 222, 'eran', 'landau');")
                .execute();

        result = keyspace
                .prepareCqlStatement()
                .withCql(
                        "INSERT INTO employees (empID, deptID, first_name, last_name) VALUES (111, 233, 'netta', 'landau');")
                .execute();

        result = keyspace.prepareCqlStatement()
                .withCql("SELECT * FROM employees WHERE empId=111;")
                .execute();

        Assert.assertTrue(!result.getResult().getRows(CQL3_CF).isEmpty());
        for (Row<Integer, String> row : result.getResult().getRows(CQL3_CF)) {
            Log.info("CQL Key: " + row.getKey());

            ColumnList<String> columns = row.getColumns();

            Log.info("   empid      : "
                    + columns.getIntegerValue("empid", null));
            Log.info("   deptid     : "
                    + columns.getIntegerValue("deptid", null));
            Log.info("   first_name : "
                    + columns.getStringValue("first_name", null));
            Log.info("   last_name  : "
                    + columns.getStringValue("last_name", null));
        }
    }

    //@Test
    public void testPreparedCql() throws Exception {
        OperationResult<CqlResult<Integer, String>> result;

        final String INSERT_STATEMENT = "INSERT INTO employees (empID, deptID, first_name, last_name) VALUES (?, ?, ?, ?);";

        result = keyspace.prepareQuery(CQL3_CF)
                .withCql(INSERT_STATEMENT)
                .asPreparedStatement()
                .withIntegerValue(222)
                .withIntegerValue(333)
                .withStringValue("Netta")
                .withStringValue("Landau")
                .execute();

        result = keyspace.prepareQuery(CQL3_CF)
                .withCql("SELECT * FROM employees WHERE empId=222;")
                .execute();
        Assert.assertTrue(!result.getResult().getRows().isEmpty());
        for (Row<Integer, String> row : result.getResult().getRows()) {
            Log.info("CQL Key: " + row.getKey());

            ColumnList<String> columns = row.getColumns();

            Log.info("   empid      : "
                    + columns.getIntegerValue("empid", null));
            Log.info("   deptid     : "
                    + columns.getIntegerValue("deptid", null));
            Log.info("   first_name : "
                    + columns.getStringValue("first_name", null));
            Log.info("   last_name  : "
                    + columns.getStringValue("last_name", null));
        }
    }

    @Test
    public void testKeyspaceCql() throws Exception {
        keyspace.prepareQuery(CQL3_CF)
                .withCql(
                        "INSERT INTO employees (empID, deptID, first_name, last_name) VALUES (999, 233, 'arielle', 'landau');")
                .execute();

        CqlStatementResult result = keyspace.prepareCqlStatement()
                .withCql("SELECT * FROM employees WHERE empID=999;")
                .execute().getResult();

        CqlSchema schema = result.getSchema();
        Rows<Integer, String> rows = result.getRows(CQL3_CF);

        Assert.assertEquals(1, rows.size());
        // Assert.assertTrue(999 == rows.getRowByIndex(0).getKey());

    }

   @Test
    public void testCollections() throws Exception {
        OperationResult<CqlStatementResult> result;
        result = keyspace
                .prepareCqlStatement()
                .withCql(
                        "INSERT INTO users (id, given, surname, favs) VALUES ('jsmith', 'John', 'Smith', { 'fruit' : 'apple', 'band' : 'Beatles' })")
                .execute();

        Rows<String, String> rows = keyspace.prepareCqlStatement()
                .withCql("SELECT * FROM users;").execute().getResult()
                .getRows(User_CF);

        MapSerializer<String, String> mapSerializer = new MapSerializer<String, String>(
                UTF8Type.instance, UTF8Type.instance);

        for (Row<String, String> row : rows) {
            Log.info(row.getKey());
            for (Column<String> column : row.getColumns()) {
                Log.info("  " + column.getName());
            }
            Column<String> favs = row.getColumns().getColumnByName("favs");
            Map<String, String> map = favs.getValue(mapSerializer);
            for (Entry<String, String> entry : map.entrySet()) {
                Log.info(" fav: " + entry.getKey() + " = " + entry.getValue());
            }
        }
    }

    @Test
    public void testUUIDPart() throws Exception {
        CqlStatementResult result;
        keyspace.prepareCqlStatement()
                .withCql(
                        "CREATE TABLE uuidtest (id UUID PRIMARY KEY, given text, surname text);")
                .execute();
        keyspace.prepareCqlStatement()
                .withCql(
                        "INSERT INTO uuidtest (id, given, surname) VALUES (00000000-0000-0000-0000-000000000000, 'x', 'arielle');")
                .execute();
        result = keyspace.prepareCqlStatement()
                .withCql("SELECT given,surname FROM uuidtest ;").execute()
                .getResult();

        Rows<UUID, String> rows = result.getRows(UUID_CF);
        Iterator<Row<UUID, String>> iter = rows.iterator();
        while (iter.hasNext()) {
            Row<UUID, String> row = iter.next();
            ColumnList<String> cols = row.getColumns();
            Iterator<Column<String>> colIter = cols.iterator();
            while (colIter.hasNext()) {
                Column<String> col = colIter.next();
                String name = col.getName();
                Log.info("*************************************");
                if (name.equals("given")) {
                    String val = col.getValue(StringSerializer.get());
                    Log.info("columnname=  " + name + "  columnvalue= " + val);
                    Assert.assertEquals("x", val);
                }
                if (name.equals("surname")) {
                    String val = col.getValue(StringSerializer.get());
                    Log.info("columnname=  " + name + "  columnvalue= " + val);
                    Assert.assertEquals("arielle", val);
                }
            }
            Log.info("*************************************");
        }
        Assert.assertEquals(1, rows.size());
    }

    @Test
    public void testUUID() throws Exception {
        keyspace.prepareCqlStatement()
                .withCql(
                        "CREATE TABLE uuidtest1 (id UUID PRIMARY KEY, given text, surname text);")
                .execute();
        keyspace.prepareCqlStatement()
                .withCql(
                        "INSERT INTO uuidtest1 (id, given, surname) VALUES (00000000-0000-0000-0000-000000000000, 'x', 'arielle');")
                .execute();
        CqlStatementResult result = keyspace.prepareCqlStatement()
                .withCql("SELECT * FROM uuidtest1 ;").execute().getResult();

        Rows<UUID, String> rows = result.getRows(UUID_CF);
        Iterator<Row<UUID, String>> iter = rows.iterator();
        while (iter.hasNext()) {
            Row<UUID, String> row = iter.next();
            ColumnList<String> cols = row.getColumns();
            Iterator<Column<String>> colIter = cols.iterator();
            while (colIter.hasNext()) {
                Column<String> col = colIter.next();
                String name = col.getName();
                Log.info("*************************************");
                if (name.equals("id")) {
                    UUID val = col.getValue(UUIDSerializer.get());
                    Log.info("columnname=  " + name + "  columnvalue= " + val);
                    Assert.assertEquals("00000000-0000-0000-0000-000000000000",
                            val.toString());
                }
                if (name.equals("given")) {
                    String val = col.getValue(StringSerializer.get());
                    Log.info("columnname=  " + name + "  columnvalue= "
                            + val.toString());
                    Assert.assertEquals("x", val);
                }
                if (name.equals("surname")) {
                    String val = col.getValue(StringSerializer.get());
                    Log.info("columnname=  " + name + "  columnvalue= " + val);
                    Assert.assertEquals("arielle", val);
                }
            }
            Log.info("*************************************");
        }
        Assert.assertEquals(1, rows.size());
    }
    
    private static void dropColumnFamiles(Keyspace keyspace, String ... cfNames) {
        for (String cfName : cfNames) {
            try { 
                System.out.println("DROPPING CF: " + cfName);
                keyspace.dropColumnFamily(cfName);
            } catch (Exception e) {
            } finally {
                try { 
                    Thread.sleep(10*1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
