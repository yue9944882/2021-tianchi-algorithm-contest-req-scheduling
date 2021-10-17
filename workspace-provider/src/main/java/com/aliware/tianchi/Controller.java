package com.aliware.tianchi;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Controller {

	private static final Logger log = LoggerFactory.getLogger(Controller.class);

	public Controller(WorkerPool pool) {
		this.watermark = 0;
		this.pool = pool;
		this.workerAutoscaler = Executors.newSingleThreadScheduledExecutor();
		this.pid = new MiniPID(20D, 0.0D, 10D);
		this.pid.setSetpoint(THRESHOLD);
		this.workerAutoscaler.scheduleAtFixedRate(() -> {
			if (!isRefreshed.get()) {
				return;
			}
			if (costs.size() < MIN_SAMPLE) {
				return;
			}
			int watermark = this.watermark;
			double velocity = pid.getOutput(watermark, THRESHOLD);
			log.info("WORKER WATERMARK 50MS: {}/{}", watermark, velocity);
			if (velocity < 0) {
				this.pool.revokeWorker();
			}
			if (velocity > 0) {
				this.pool.addWorker();
			}
			isRefreshed.set(false);
		}, INTERVAL, INTERVAL, TimeUnit.MILLISECONDS);
	}

	private static final int CAPACITY = 3000;
	private static final int THRESHOLD = 100;
	private static final long INTERVAL = 20;
	private static final int MIN_SAMPLE = 2000;

	private final LinkedList<Long> costs = new LinkedList<>();
	private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
	private final ScheduledExecutorService workerAutoscaler;
	private final AtomicBoolean isRefreshed = new AtomicBoolean(true);
	private final WorkerPool pool;
	private final MiniPID pid;
	public long sum;
	private int watermark;

	public void record(long cost) {
		this.rwLock.writeLock().lock();
		if (costs.size() == CAPACITY) {
			long removed = costs.removeFirst();
			if (removed >= 8000) {
				watermark--;
			}
			sum -= removed;
		}
		costs.addLast(cost);
		if (cost >= 8000) {
			watermark++;
		}
		sum += cost;
		this.rwLock.writeLock().unlock();
		isRefreshed.set(true);
	}

	public long avg() {
		this.rwLock.readLock().lock();
		try {
			if (costs.size() == 0) {
				return 0;
			}
			return sum / costs.size();
		}
		finally {
			this.rwLock.readLock().unlock();
		}
	}

}
