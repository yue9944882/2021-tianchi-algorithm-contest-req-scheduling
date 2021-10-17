package io.yue9944882.flowcontrol.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.yue9944882.flowcontrol.loadbalance.Registry;
import io.yue9944882.flowcontrol.prober.Prober;
import io.yue9944882.flowcontrol.traffic.TrafficControlRequest;
import io.yue9944882.flowcontrol.window.Digest;
import io.yue9944882.flowcontrol.window.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ammo {

	private static final Logger log = LoggerFactory.getLogger(Ammo.class);

	public Ammo(Window window, Registry registry, Prober prober) {
		this.window = window;
		this.bucket = new Bucket();
		this.registry = registry;
		this.prober = prober;
	}

	private final Window window;
	private final ExecutorService processor = Executors.newSingleThreadExecutor();
	private final Bucket bucket;
	private final Registry registry;
	private final Prober prober;

	public void start() {
		processor.submit(() -> {
			while (true) {
				Thread.sleep(1L);
				long start = System.currentTimeMillis();
				try {
					Window.Snapshot s = window.emptySnapshot();
					if (s.getRecalls().length == 0) {
						Gatlin.getInstance().getWindow().waitUntilReady();
						continue;
					}
					int mod = registry.count();
					Map<Integer, BucketShard> prevShards = bucket.items.get();
					Map<Integer, BucketShard> newShards = new HashMap<>();
					Map<Integer, List<Digest>> scheduled = new HashMap<>();
					Map<Integer, Integer> thisRoundCount = new HashMap<>();
					Map<Integer, Long> avgs = new HashMap<>();
					Map<Integer, Boolean> healthy = new HashMap<>();
					for (int i = 0; i < mod; i++) {
						scheduled.putIfAbsent(i, new ArrayList<>());
						thisRoundCount.putIfAbsent(i, 0);
						avgs.put(i, 0L);
						healthy.put(i, prober.isHealthy(i));
					}

					// collect avg
					scheduled.entrySet().stream()
						.forEach(kv -> {
							avgs.put(kv.getKey(), registry.getAvg(kv.getKey()));
						});
					// stable sort
					List<Digest> unscheduled = new ArrayList<>();

					for (Digest d : s.getRecalls()) {
						boolean alreadyScheduled = false;
						for (int j = 0; j < registry.count(); j++) {
							BucketShard prevShard = prevShards.get(j);
							if (prevShard == null) {
								break;
							}
							if (prevShard.index == null) {
								unscheduled.add(d);
								continue;
							}
							if (prevShard.index.contains(d.getSeq())) {
								scheduled.get(j).add(d);
								alreadyScheduled = true;
								break;
							}
						}
						if (!alreadyScheduled) {
							unscheduled.add(d);
						}
					}

					for (int i = 0; i < mod; i++) {
						if (!healthy.get(i)) {
							unscheduled.addAll(scheduled.get(i));
						}
					}

					int unscheduledCount = unscheduled.size();
					int targetCount = 0;

					Map<Integer, Integer> vts = new HashMap<>();
					for (int i = 0; i < mod; i++) {
						vts.put(i, avgs.get(i).intValue() * scheduled.get(i).size());
					}
					int robinPtr = ThreadLocalRandom.current().nextInt(mod);
					for (int i = 0; i < unscheduledCount; i++) {
						int min = -1;
						int minVt = Integer.MAX_VALUE;
						for (int j = 0; j < mod; j++) {
							if (!healthy.get(j)) {
								continue;
							}
							int ptr = (robinPtr + j) % mod;
							int vt = vts.get(ptr);
							if (vt < minVt) {
								minVt = vt;
								min = ptr;
							}
						}
						if (min == -1) {
							break;
						}
						int finalMin = min;
						vts.compute(min, (k, v) -> v + avgs.get(finalMin).intValue());
						scheduled.get(min).add(unscheduled.get(i));
						thisRoundCount.compute(min, (k, v) -> v + 1);
						robinPtr++;
					}

					// schedule
					int scheduledCount = scheduled.values().stream()
						.map(bucketShard -> bucketShard.size())
						.reduce(0, Integer::sum);


					// refresh
					log.info("SHARD TARGET WIN [{}<<{}]({}) from {} to {} in {}: {}",
						0, 0,
						avgs.entrySet().stream()
							.map(kv -> {
								return kv.getKey() + "=" + kv.getValue();
							})
							.collect(Collectors.joining(",")),
						scheduledCount, targetCount, unscheduledCount,
						scheduled.entrySet().stream()
							.map(kv -> {
								return kv.getKey()
									+ "==>(+" + thisRoundCount.get(kv.getKey()) + ")" + kv.getValue().size();
							})
							.collect(Collectors.joining(",")));
					scheduled.entrySet().stream().forEach(kv -> {
						newShards.putIfAbsent(kv.getKey(), new BucketShard());
						newShards.get(kv.getKey()).items = kv.getValue().toArray(new Digest[0]);
						newShards.get(kv.getKey()).index = kv.getValue().stream()
							.map(d -> d.getSeq())
							.collect(Collectors.toSet());
					});
					bucket.items.set(newShards);
				}
				catch (Throwable t) {
					log.info("AMMO PROCESSOR ABORT: {} {}", t.getMessage(), t.getStackTrace());
				}
				finally {
					long cost = System.currentTimeMillis() - start;
					if (cost > 50) {
						log.info("AMMO REFRESH SLOW {}", cost);
					}
				}
			}
		});
	}

	public void inject(int shard, TrafficControlRequest req) {
		bucket.inject(shard, req);
	}

	public class Bucket {

		private final AtomicReference<Map<Integer, BucketShard>> items = new AtomicReference<>(new HashMap<>());

		private void inject(int shard, TrafficControlRequest req) {
			BucketShard bucketShard = items.get().get(shard);
			if (bucketShard == null || bucketShard.items == null || bucketShard.items.length == 0) {
				return;
			}
			int[] seqs = new int[bucketShard.items.length];
			for (int i = 0; i < bucketShard.items.length; i++) {
				seqs[i] = bucketShard.items[i].getSeq();
			}
			req.appendIndex(seqs);
			Arrays.stream(bucketShard.items).forEach(req::appendRecall);
		}
	}

	public class BucketShard {
		private Digest[] items;
		private Set<Integer> index;
	}
}
