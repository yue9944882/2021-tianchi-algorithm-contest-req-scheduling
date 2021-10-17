package io.yue9944882.flowcontrol.window;

import java.util.Collection;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.yue9944882.flowcontrol.basic.Request;
import io.yue9944882.flowcontrol.basic.RpcRequest;
import io.yue9944882.flowcontrol.param.Parameters;
import org.apache.dubbo.rpc.Invocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Window {

	private static final Logger log = LoggerFactory.getLogger(Window.class);

	public Window() {
		this.lockMod = Parameters.GATLIN_WINDOW_LOCKMOD;
		this.locks = new ReadWriteLock[lockMod];
		this.trackings = new TreeMap[lockMod];
		for (int i = 0; i < lockMod; i++) {
			this.locks[i] = new ReentrantReadWriteLock();
			this.trackings[i] = new TreeMap<>();
		}
	}

	private static final AtomicInteger serial = new AtomicInteger();
	private final int lockMod;
	private final ReadWriteLock[] locks;
	private final TreeMap<Integer, Digest>[] trackings;
	private final AtomicBoolean ready = new AtomicBoolean(false);

	public Request start(Invocation inv) {
		int seq = serial.incrementAndGet();
		RpcRequest req = new RpcRequest(seq, inv);
		Digest digest = new Digest(seq, req.getInput());

		this.locks[seq % lockMod].writeLock().lock();
		this.trackings[seq % lockMod].put(digest.getSeq(), digest);
		this.locks[seq % lockMod].writeLock().unlock();

		synchronized (this) {
			ready.set(true);
			this.notifyAll();
		}
		return req;
	}

	public void finish(int seq, String reason) {
		this.locks[seq % lockMod].writeLock().lock();
		this.trackings[seq % lockMod].remove(seq);
		this.locks[seq % lockMod].writeLock().unlock();
	}

	public Snapshot emptySnapshot() {
		TreeMap<Integer, Digest> items = new TreeMap<>();
		for (int i = 0; i < lockMod; i++) {
			if (this.trackings[i].size() == 0) {
				continue;
			}
			this.locks[i].readLock().lock();
			this.trackings[i].forEach(items::put);
			this.locks[i].readLock().unlock();
		}
		Snapshot snapshot = new Snapshot(
			false,
			items);
		return snapshot;
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
