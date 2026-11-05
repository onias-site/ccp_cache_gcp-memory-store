package com.ccp.implementations.cache.gcp.memorystore;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import com.ccp.aop.CcpAllowNullReturn;
import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpErrorInputStreamMissing;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.especifications.cache.CcpCache;
import com.ccp.especifications.db.utils.CcpDbCallsOfTheThread;

import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.JedisPooled;

/**
 * {@code CcpCache} implementation backed by Memorystore for Valkey, reached through a single endpoint (cluster mode
 * disabled). Every value is stored as a JSON text with the shape <code>{"value": ...}</code>, so a text that happens to
 * be a JSON stays a text; a map is read back as {@code CcpJsonRepresentation}.
 * <p>
 * A failure of the server is thrown to the caller instead of being read as a miss: a delete that fails silently would
 * leave a stale record served until it expires.
 */
class GcpMemoryStore implements CcpCache {
	enum Fields implements CcpJsonFieldName {
		application_properties,
		localhost,
		value,
		memorystore_host {
			public String getValue() {
				return "memorystore.host";
			}
		},
		memorystore_port {
			public String getValue() {
				return "memorystore.port";
			}
		},
	}

	/** The port of Memorystore for Valkey. */
	private static final int DEFAULT_PORT = 6379;

	/** The default concurrency of a Cloud Run instance: no request waits for a connection. */
	private static final int MAX_CONNECTIONS = 80;

	/** How long a request waits for a free connection before failing. */
	private static final Duration MAX_WAIT_FOR_A_CONNECTION = Duration.ofSeconds(2);

	/** The single instance; the connections open on the first command. */
	public final static GcpMemoryStore INSTANCE = new GcpMemoryStore(createClient());

	/** The pool of connections to the server. */
	private final JedisPooled client;

	/**
	 * Creates the cache over a client.
	 * @param client the pool of connections to the server
	 */
	GcpMemoryStore(JedisPooled client) {
		this.client = client;
	}

	/**
	 * Builds the client from {@code memorystore.host} and {@code memorystore.port} of {@code application_properties}.
	 * @return the pool of connections
	 */
	private static JedisPooled createClient() {
		CcpJsonRepresentation properties = getConnectionProperties();
		String host = properties.getAsString(Fields.memorystore_host);
		Integer port = properties.getAsIntegerNumber(Fields.memorystore_port);
		ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
		poolConfig.setMaxTotal(MAX_CONNECTIONS);
		poolConfig.setMaxIdle(MAX_CONNECTIONS);
		poolConfig.setMaxWait(MAX_WAIT_FOR_A_CONNECTION);
		JedisPooled pooled = new JedisPooled(poolConfig, host, port);
		return pooled;
	}

	/**
	 * Reads the connection properties over the defaults ({@code localhost:6379}).
	 * @return the connection properties
	 */
	private static CcpJsonRepresentation getConnectionProperties() {
		CcpJsonRepresentation defaults = CcpOtherConstants.EMPTY_JSON
				.put(Fields.memorystore_host, Fields.localhost.getValue())
				.put(Fields.memorystore_port, DEFAULT_PORT);
		try {
			String propertiesName = Fields.application_properties.getValue();
			CcpStringDecorator propertiesFileName = new CcpStringDecorator(propertiesName);
			CcpPropertiesDecorator propertiesDecorator = propertiesFileName.propertiesFrom();
			CcpJsonRepresentation systemProperties = propertiesDecorator.environmentVariablesOrClassLoaderOrFile();
			CcpJsonRepresentation properties = defaults.mergeWithAnotherJson(systemProperties);
			return properties;
		} catch (CcpErrorInputStreamMissing e) {
			return defaults;
		}
	}

	/**
	 * Reads the value of the key; a cached map is returned as {@code CcpJsonRepresentation}.
	 * @param key the cache key
	 * @return the value, or {@code null} on a miss
	 */
	@SuppressWarnings("unchecked")
	@CcpAllowNullReturn
	public Object get(String key) {
		long started = System.nanoTime();
		String storedText = this.client.get(key);
		boolean missing = storedText == null;
		double clientMs = elapsedMs(started);
		CcpDbCallsOfTheThread.recordCacheRead(key, false == missing, clientMs);

		if (missing) {
			return null;
		}

		CcpJsonRepresentation storedJson = new CcpJsonRepresentation(storedText);
		Object value = storedJson.get(Fields.value);
		boolean isMap = value instanceof Map;
		boolean isNotMap = false == isMap;

		if (isNotMap) {
			return value;
		}

		Map<String, Object> map = (Map<String, Object>) value;
		CcpJsonRepresentation cachedJson = new CcpJsonRepresentation(map);
		return cachedJson;
	}

	/**
	 * Stores the value with the expiration; a {@code CcpJsonRepresentation} is stored as its map.
	 * @param key the cache key
	 * @param value the value
	 * @param secondsDelay expiration in seconds
	 * @return this cache
	 */
	public CcpCache put(String key, Object value, int secondsDelay) {
		boolean isCcpJsonRepresentation = value instanceof CcpJsonRepresentation;
		Object valueToStore = isCcpJsonRepresentation ? ((CcpJsonRepresentation) value).content : value;
		CcpJsonRepresentation storedJson = CcpOtherConstants.EMPTY_JSON.put(Fields.value, valueToStore);
		String storedText = storedJson.asUgglyJson();
		long started = System.nanoTime();
		this.client.setex(key, secondsDelay, storedText);
		double clientMs = elapsedMs(started);
		CcpDbCallsOfTheThread.recordCacheWrite(clientMs);
		return this;
	}

	/**
	 * Deletes the key.
	 * @param key the cache key
	 */
	public void delete(String key) {
		long started = System.nanoTime();
		this.client.del(key);
		double clientMs = elapsedMs(started);
		CcpDbCallsOfTheThread.recordCacheWrite(clientMs);
	}

	/**
	 * Deletes all keys in a single command instead of one command per key; the server refuses a command without keys.
	 * @param keys the keys to delete
	 */
	public void deleteAll(Collection<String> keys) {
		boolean nothingToDelete = keys.isEmpty();

		if (nothingToDelete) {
			return;
		}

		int keysCount = keys.size();
		String[] keysToDelete = keys.toArray(new String[keysCount]);
		long started = System.nanoTime();
		this.client.del(keysToDelete);
		double clientMs = elapsedMs(started);
		CcpDbCallsOfTheThread.recordCacheWrite(clientMs);
	}

	/**
	 * The time since a moment, for the measurement of the commands ({@link CcpDbCallsOfTheThread}).
	 * @param started the moment, from {@link System#nanoTime()}
	 * @return the milliseconds since it
	 */
	private static double elapsedMs(long started) {
		long elapsedNanos = System.nanoTime() - started;
		double elapsedMs = elapsedNanos / 1_000_000.0;
		return elapsedMs;
	}

}
