package com.ccp.implementations.cache.gcp.memorystore;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.cache.CcpCache;

/**
 * DI provider that exposes GcpMemoryStore as the CcpCache implementation on Google Cloud Platform.
 */
public class CcpGcpMemoryStore implements CcpInstanceProvider<CcpCache> {
	/** The single instance: the class keeps no state. */
	public final static CcpGcpMemoryStore INSTANCE = new CcpGcpMemoryStore();

	private CcpGcpMemoryStore() {}

	/**
	 * Builds the Memorystore implementation of {@code CcpCache}.
	 * @return the {@code GcpMemoryStore}
	 */
	public CcpCache getInstance() {
		GcpMemoryStore gcpMemoryStore = GcpMemoryStore.INSTANCE;
		return gcpMemoryStore;
	}

}
