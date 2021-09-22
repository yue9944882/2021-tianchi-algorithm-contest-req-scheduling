package com.aliware.tianchi;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import io.yue9944882.flowcontrol.window.Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

public class WorkQueue {

	private static final Logger log = LoggerFactory.getLogger(WorkQueue.class);

	private final Map<Integer, List<Work>> seqIndex = new HashMap<>();
	private static final int scale = 2000;

	private final TreeMap<Work, DigestWithInvocation> inputs = new TreeMap<>((o1, o2) -> {
		int cmpScale = Integer.compare(o1.seq / scale, o2.seq / scale);
		if (cmpScale != 0) {
			return cmpScale;
		}
		int cmpPriority = Integer.compare(o1.priority, o2.priority);
		if (cmpPriority != 0) {
			return cmpPriority;
		}
		return Integer.compare(o1.seq, o2.seq);
	});
	private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
	private final int maxSize = 3000;

	public void trim(int[] index, long deadlineMillis) {
		final int maxTrimCount = 20;
		Set<Integer> undos = new HashSet<>();
		for (int i : index) {
			undos.add(i);
		}
		rwLock.writeLock().lock();
		NavigableSet<Work> keys = this.inputs.navigableKeySet();
		rwLock.writeLock().unlock();

		Iterator<Work> iter = keys.iterator();
		int count = 0;
		while (iter.hasNext() && System.currentTimeMillis() < deadlineMillis) {
			if (count >= maxTrimCount) {
				break;
			}
			Work work = iter.next();
			if (!undos.contains(work.seq)) {
				if (this.inputs.remove(work) != null) {
					count++;
				}
			}
		}
		if (count > 0) {
			log.info("CLEAN {}", count);
		}
	}

	public boolean push(DigestWithInvocation input, boolean requeue) {
		rwLock.writeLock().lock();
		try {
			if (this.inputs.size() >= maxSize && !requeue) {
				return false;
			}
			Work w = new Work(input.getSeq(), input.getPriority());
			this.seqIndex.putIfAbsent(input.getSeq(), new ArrayList<>());
			this.seqIndex.get(input.getSeq()).add(w);
			this.inputs.put(w, input);
			return true;
		}
		finally {
			rwLock.writeLock().unlock();
		}
	}

	public DigestWithInvocation pop() {
		rwLock.writeLock().lock();
		try {
			try {
				Map.Entry<Work, DigestWithInvocation> entry = this.inputs.pollFirstEntry();
				if (entry == null) {
					return null;
				}
				List<Work> works = this.seqIndex.get(entry.getKey().seq);
				works.remove(entry.getKey());
				if (works.size() == 0) {
					this.seqIndex.remove(entry.getKey().seq);
				}
				return entry.getValue();
			}
			catch (NoSuchElementException e) {
				return null;
			}
		}
		finally {
			rwLock.writeLock().unlock();
		}
	}

	public int size() {
		this.rwLock.readLock().lock();
		try {
			return this.inputs.size();
		}
		finally {
			this.rwLock.readLock().unlock();
		}
	}

	public boolean contains(DigestWithInvocation input) {
		this.rwLock.readLock().lock();
		try {
			List<Work> works = this.seqIndex.get(input.getSeq());
			if (works == null) {
				return false;
			}
			return works.contains(new Work(input.getSeq(), input.getPriority()));
		}
		finally {
			this.rwLock.readLock().unlock();
		}
	}

	public static class Work {

		private final int seq;
		private final int priority;

		public Work(int seq, int priority) {
			this.seq = seq;
			this.priority = priority;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Work work = (Work) o;
			return seq == work.seq && priority == work.priority;
		}

		@Override
		public int hashCode() {
			return Objects.hash(seq, priority);
		}
	}

}
