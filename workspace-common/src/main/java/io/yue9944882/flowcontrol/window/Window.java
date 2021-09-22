package io.yue9944882.flowcontrol.window;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import io.yue9944882.flowcontrol.basic.Request;
import io.yue9944882.flowcontrol.basic.Response;
import io.yue9944882.flowcontrol.basic.RpcRequest;
import org.apache.dubbo.common.utils.LRUCache;
import org.apache.dubbo.rpc.Invocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Window {

	private static final Logger log = LoggerFactory.getLogger(Window.class);

	public Window() {
		this.tracking = new TreeMap<>(Integer::compare);
		this.lock = new ReentrantReadWriteLock();
		this.starts = new HashMap<>();
	}

	private static final int maxBulkSize = 10;
	private static final int pushWindowSize = 3;

	private static final AtomicInteger serial = new AtomicInteger();
	private final TreeMap<Integer, Digest> tracking;
	private final Map<Integer, OffsetDateTime> starts;
	private final ReadWriteLock lock;

	public Request start(Invocation inv, OffsetDateTime start) {
		int seq = serial.incrementAndGet();
		log.info("[{}] WINDOW START {}", start, seq);
		RpcRequest req = new RpcRequest(seq, inv);
		Digest digest = new Digest(seq, req.getInput());

		this.lock.writeLock().lock();
		this.starts.put(seq, start);
		this.tracking.put(digest.getSeq(), digest);
		this.lock.writeLock().unlock();
		return req;
	}

	public void finish(int seq, String reason) {
		this.lock.writeLock().lock();
		this.tracking.remove(seq);
		OffsetDateTime start = this.starts.remove(seq);
		if (start != null) {
			log.info("[{}] WINDOW FINISH {} REASON {}, COST {}",
				OffsetDateTime.now(),
				seq,
				reason,
				OffsetDateTime.now().toInstant().toEpochMilli() - start.toInstant().toEpochMilli());
		}
		this.lock.writeLock().unlock();
	}

	public boolean isFinished(int seq) {
		this.lock.readLock().lock();
		try {
			Integer start = this.tracking.firstKey();
			if (start == null) {
				return true;
			}
			if (start > seq) {
				return true;
			}
			return !this.tracking.containsKey(seq);
		}
		catch (NoSuchElementException e) {
			return true;
		}
		finally {
			this.lock.readLock().unlock();
		}
	}

	public Digest getDigestOrNull(int seq) {
		this.lock.readLock().lock();
		try {
			return this.tracking.get(seq);
		}
		finally {
			this.lock.readLock().unlock();
		}
	}

	public int leftSeq() {
		this.lock.readLock().lock();
		try {
			return this.tracking.firstKey();
		}
		catch (NoSuchElementException e) {
			return -1;
		}
		finally {
			this.lock.readLock().unlock();
		}
	}

	public int rightSeq() {
		this.lock.readLock().lock();
		try {
			return this.tracking.lastKey();
		}
		catch (NoSuchElementException e) {
			return -1;
		}
		finally {
			this.lock.readLock().unlock();
		}
	}

	public Snapshot emptySnapshot() {
		this.lock.readLock().lock();
		try {
			Set<Integer> pending = new HashSet<>(this.tracking.keySet());
			return new Snapshot(
				false,
				this.leftSeq(),
				this.rightSeq(),
				new TreeMap<>());
		}
		finally {
			this.lock.readLock().unlock();
		}
	}

	public static class Snapshot {

		public Snapshot(boolean isEmpty, int left, int right, TreeMap<Integer, Digest> pendings) {
			this.isEmpty = isEmpty;
			this.left = left;
			this.right = right;
			this.pendings = pendings;
		}

		private final boolean isEmpty;
		private final int left;
		private final int right;
		private final TreeMap<Integer, Digest> pendings;

		public int getLeft() {
			return left;
		}

		public int getRight() {
			return right;
		}

		public Set<Integer> getPendings() {
			return this.pendings.keySet();
		}

		public Collection<Digest> getAll() {
			return this.pendings.values();
		}

		public Digest[] getPushes() {
			if (isEmpty) {
				return new Digest[0];
			}
			return new Digest[] {this.pendings.get(left)};
		}

		public Digest[] getRecalls(int seq, int idx, int total) {
			List<Digest> bulk = new ArrayList<>();
			Integer runner = seq;
			for (int i = 0; i < maxBulkSize; i++) {
				runner = this.pendings.lowerKey(runner);
				if (runner == null) {
					break;
				}
				if (runner % total != idx) {
					continue;
				}
				Digest digest = this.pendings.get(runner);
				if (digest != null) {
					bulk.add(digest);
				}
			}
			return bulk.toArray(new Digest[0]);
		}

		public Digest get(int seq) {
			return pendings.get(seq);
		}
	}

}
