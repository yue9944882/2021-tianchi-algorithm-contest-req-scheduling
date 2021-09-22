package io.yue9944882.flowcontrol.loadbalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import io.yue9944882.flowcontrol.client.Clients;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvokerRegistry implements Registry {

	private static final Logger log = LoggerFactory.getLogger(InvokerRegistry.class);

	public InvokerRegistry(List<Invoker> invokers) {
		init(invokers);
	}

	private final List<String> candidates = new ArrayList<>();
	private final Map<Integer, Long> avg = new ConcurrentHashMap<>();
	private boolean init = false;

	@Override
	public int indexOf(Invoker invoker) {
		String key = Clients.getName(invoker);
		for (int i = 0; i < candidates.size(); i++) {
			if (candidates.get(i).equals(key)) {
				return i;
			}
		}
		return 0;
	}

	@Override
	public int count() {
		return candidates.size();
	}

	@Override
	public void setAvg(int shard, long avgCost) {
		avg.put(shard, avgCost);
	}

	@Override
	public long getAvg(int shard) {
		return avg.getOrDefault(shard, 0L);
	}

	private Invoker next(List<Invoker> invokers) {
		int idx = ThreadLocalRandom.current().nextInt(invokers.size());
		String key = this.candidates.get(idx);
		return invokers.stream()
			.filter(i -> Clients.getName(i).equals(key))
			.findFirst()
			.get();
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
						for (int i = 0; i < candidates.size(); i++) {
							log.info("RR INIT: {} -> {}", candidates.get(i), i);
						}
					}
					init = true;
				}
			}
		}
	}
}
