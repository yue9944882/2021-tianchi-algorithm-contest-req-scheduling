package io.yue9944882.flowcontrol.loadbalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.yue9944882.flowcontrol.client.Clients;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvokerRegistry implements Registry {

	private static final Logger log = LoggerFactory.getLogger(InvokerRegistry.class);

	public InvokerRegistry(List<Invoker> invokers) {
		init(invokers);
	}

	private final Map<String, Integer> index = new HashMap<>();
	private final List<String> candidates = new ArrayList<>();
	private final Map<Integer, ReadWriteLock> avgLock = new HashMap<>();
	private final Map<Integer, Long> avg = new HashMap<>();
	private boolean init = false;

	@Override
	public int indexOf(Invoker invoker) {
		String key = Clients.getName(invoker);
		return index.get(key);
	}

	@Override
	public int count() {
		return candidates.size();
	}

	@Override
	public void setAvg(int shard, long avgCost) {
		avgLock.get(shard).writeLock().lock();
		avg.put(shard, avgCost);
		avgLock.get(shard).writeLock().unlock();
	}

	@Override
	public long getAvg(int shard) {
		avgLock.get(shard).readLock().lock();
		try {
			return avg.get(shard);
		}
		finally {
			avgLock.get(shard).readLock().unlock();
		}
	}

	private void init(List<Invoker> invokers) {
		if (!init) {
			synchronized (this) {
				if (!init) {
					for (Invoker invoker : invokers) {
						String key = Clients.getName(invoker);
						if (!candidates.contains(key)) {
							candidates.add(key);
						}
					}
					for (int i = 0; i < candidates.size(); i++) {
						index.put(candidates.get(i), i);
						avgLock.put(i, new ReentrantReadWriteLock());
						avg.put(i, 0L);
						log.info("RR INIT: {} -> {}", candidates.get(i), i);
					}
					init = true;
				}
			}
		}
	}
}
