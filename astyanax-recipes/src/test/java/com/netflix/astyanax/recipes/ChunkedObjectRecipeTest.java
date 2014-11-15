package com.netflix.astyanax.recipes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import com.netflix.astyanax.recipes.storage.ObjectWriteCallback;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.CassandraTestServerProxy;

public class ChunkedObjectRecipeTest {

	private static final Logger LOG = LoggerFactory.getLogger(ChunkedObjectRecipeTest.class);
	
	public static ColumnFamily<String, String> CF_CHUNK = 
			ColumnFamily.newColumnFamily("cfchunk", StringSerializer.get(), StringSerializer.get());

	/**
	 * Interal
	 */
	private static Keyspace                  keyspace;

	@BeforeClass
	public static void setup() throws Exception {

	    CassandraTestServerProxy.getInstance().startCassServer();
		createKeyspace();
	}

	@AfterClass
	public static void teardown() throws Exception {
	}

	public static void createKeyspace() throws Exception {

	    keyspace = 
	            CassandraTestServerProxy.getInstance().getOrCreateKeyspace(ThriftFamilyFactory.getInstance());

		keyspace.createColumnFamily(CF_CHUNK,  null);
	}

	@Test
	public void testChunkedRecipe() throws Exception {

		CassandraChunkedStorageProvider provider = new CassandraChunkedStorageProvider(keyspace, CF_CHUNK);

		StringBuilder sb = new StringBuilder();
		for (int i=0; i<100; i++) {
			sb.append("abcdefghijklmnopqrstuvwxyz_");
		}

		String input = sb.toString();

		ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());

		ObjectMetadata meta = ChunkedStorage.newWriter(provider, "MyObject", in)
				.withChunkSize(100)
				.call();

		meta = ChunkedStorage.newInfoReader(provider, "MyObject").call();
		System.out.println("Obj size: " + meta.getObjectSize().intValue());
		System.out.println("Chunk count: " + meta.getChunkCount());

		ByteArrayOutputStream os = new ByteArrayOutputStream(meta.getObjectSize().intValue());

		meta = ChunkedStorage.newReader(provider, "MyObject", os)
				.withBatchSize(11)       // Randomize fetching blocks within a batch. 
				.withConcurrencyLevel(3)
				.call();

		String output = os.toString();

		Assert.assertEquals(input, output);

		ChunkedStorage.newDeleter(provider, "MyObject").call();

		for (int i=0; i<meta.getChunkCount(); i++) {
			ColumnList<String> result = keyspace.prepareQuery(CF_CHUNK).getKey("MyObject$" + i).execute().getResult();
			Assert.assertTrue(result.isEmpty());
		}
		
	}
	
	@Test
	public void testChunkFailure() throws Exception {

		CassandraChunkedStorageProvider provider = new CassandraChunkedStorageProvider(keyspace, CF_CHUNK);

		StringBuilder sb = new StringBuilder();
		for (int i=0; i<100; i++) {
			sb.append("abcdefghijklmnopqrstuvwxyz_");
		}

		String input = sb.toString();

		ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());

		CallbackThatFails callback = new CallbackThatFails(26);

		try {
			ChunkedStorage.newWriter(provider, "MyObjectThatFails", in)
				.withChunkSize(100)
				.withCallback(callback)
				.call();
		Assert.fail("Should have received ex from ChunkedObjectWriter");
		} catch (Exception e) {
			
		} finally {
			Assert.assertFalse("callback.success: " + callback.success, callback.success);
			Assert.assertTrue("callback.chunkException: " + callback.chunkException, callback.chunkException);
			Assert.assertEquals("callback.failedChunk: " + callback.failedChunk, callback.chunkNumToFailOn, callback.failedChunk);
		}
	}
	
	private class CallbackThatFails implements ObjectWriteCallback {
		
		private int chunkNumToFailOn;
		private boolean success = false;
		private boolean chunkException = false;
		private int failedChunk = -1;
		
		private CallbackThatFails(int chunk) {
			chunkNumToFailOn = chunk;
		}
		
		@Override
		public void onSuccess() {
			success = true;
		}
		
		@Override
		public void onFailure(Exception exception) {
		}
		
		@Override
		public void onChunkException(int chunk, Exception exception) {
			chunkException = true;
			failedChunk = chunk;
		}
		
		@Override
		public void onChunk(int chunk, int size) {
			if (chunk == chunkNumToFailOn) {
				try { 
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}
				throw new RuntimeException("Failing for chunk: " + chunkNumToFailOn);
			}
		}
	};
}
