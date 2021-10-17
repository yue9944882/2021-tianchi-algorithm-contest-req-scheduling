package com.aliware.tianchi;

import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.yue9944882.flowcontrol.param.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Controller {

	private static final Logger log = LoggerFactory.getLogger(Controller.class);

	public Controller(WorkerPool pool) {
		this.watermark = 0;
		this.pool = pool;
		this.workerAutoscaler = Executors.newSingleThreadScheduledExecutor();
		this.pid = new MiniPID(Parameters.PROVIDER_PID_P, Parameters.PROVIDER_PID_I, Parameters.PROVIDER_PID_D);
		this.pid.setSetpoint(THRESHOLD);
		this.workerAutoscaler.scheduleWithFixedDelay(() -> {
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

	private static final int CAPACITY = Parameters.PROVIDER_PID_TRACK_RING_CAPACITY;
	private static final int MIN_SAMPLE = Parameters.PROVIDER_PID_TRACK_RING_EFFECTIVE_CAPACITY;
	private static final int THRESHOLD = Parameters.PROVIDER_PID_CONTROL_DELAY_TARGET_THRESHOLD;
	private static final long INTERVAL = Parameters.PROVIDER_PID_CONTROL_DELAY_INTERVAL_MILLIS;

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
			if (removed >= Parameters.PROVIDER_PID_CONTROL_DELAY_TARGET_MILLIS) {
				watermark--;
			}
			sum -= removed;
		}
		costs.addLast(cost);
		if (cost >= Parameters.PROVIDER_PID_CONTROL_DELAY_TARGET_MILLIS) {
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
