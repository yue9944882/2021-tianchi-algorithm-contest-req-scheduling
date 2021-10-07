package com.aliware.tianchi;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
		}, INTERVAL, INTERVAL, TimeUnit.MILLISECONDS);
	}

	private static final int CAPACITY = 3000;
	private static final int THRESHOLD = 100;
	private static final long INTERVAL = 100;
	private static final int MIN_SAMPLE = 2000;

	private final LinkedList<Long> costs = new LinkedList<>();
	private final Lock lock = new ReentrantLock();
	private final ScheduledExecutorService workerAutoscaler;
	private final WorkerPool pool;
	private final MiniPID pid;
	public long sum;
	private int watermark;

	public void record(long cost) {
		lock.lock();
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
		lock.unlock();
	}

	public long avg() {
		lock.lock();
		try {
			if (costs.size() == 0) {
				return 0;
			}
			return sum / costs.size();
		}
		finally {
			lock.unlock();
		}
	}

}
