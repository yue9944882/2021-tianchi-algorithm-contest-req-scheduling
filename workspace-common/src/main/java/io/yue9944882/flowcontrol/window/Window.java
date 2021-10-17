package io.yue9944882.flowcontrol.window;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.yue9944882.flowcontrol.basic.Request;
import io.yue9944882.flowcontrol.basic.RpcRequest;
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

	private static final AtomicInteger serial = new AtomicInteger();
	private final TreeMap<Integer, Digest> tracking;
	private final Map<Integer, OffsetDateTime> starts;
	private final ReadWriteLock lock;
	private final AtomicBoolean ready = new AtomicBoolean(false);

	public Request start(Invocation inv, OffsetDateTime start) {
		int seq = serial.incrementAndGet();
		log.info("[{}] WINDOW START {}", start, seq);
		RpcRequest req = new RpcRequest(seq, inv);
		Digest digest = new Digest(seq, req.getInput());

		this.lock.writeLock().lock();
		this.starts.put(seq, start);
		this.tracking.put(digest.getSeq(), digest);
		this.lock.writeLock().unlock();

		synchronized (this) {
			ready.set(true);
			this.notifyAll();
		}
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

	public Digest getDigestOrNull(int seq) {
		this.lock.readLock().lock();
		try {
			return this.tracking.get(seq);
		}
		finally {
			this.lock.readLock().unlock();
		}
	}

	public Snapshot emptySnapshot() {
		this.lock.readLock().lock();
		try {
			Set<Integer> pending = new HashSet<>(this.tracking.keySet());
			Snapshot snapshot = new Snapshot(
				false,
				new TreeMap<>(this.tracking));
			return snapshot;
		}
		finally {
			this.lock.readLock().unlock();
		}
	}

	public void waitUntilReady() {
		synchronized (this) {
			if (this.ready.compareAndSet(true, false)) {
				return;
			}
			try {
				this.wait();
				waitUntilReady();
			}
			catch (InterruptedException e) {
				log.info("WAIT ABORT");
			}
		}
	}

	public static class Snapshot {

		public Snapshot(boolean isEmpty, TreeMap<Integer, Digest> pendings) {
			this.isEmpty = isEmpty;
			this.pendings = pendings;
		}

		private final boolean isEmpty;
		private final TreeMap<Integer, Digest> pendings;

		public Set<Integer> getPendings() {
			return this.pendings.keySet();
		}

		public Collection<Digest> getAll() {
			return this.pendings.values();
		}


		public Digest[] getRecalls() {
			return this.pendings.values().stream().toArray(Digest[]::new);
		}

		public Digest get(int seq) {
			return pendings.get(seq);
		}
	}

}
