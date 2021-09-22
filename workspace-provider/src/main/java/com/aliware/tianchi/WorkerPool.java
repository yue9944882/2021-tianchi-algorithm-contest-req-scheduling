package com.aliware.tianchi;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerPool implements Executor {

	private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

	public WorkerPool(int workerCount, int maxWorkerCount) {
		this.minWorkerCount = workerCount;
		this.maxWorkerCount = maxWorkerCount;
		this.currentWorkerCount = workerCount;
		this.sem = new Semaphore(maxWorkerCount);
		this.scheduler = Executors.newFixedThreadPool(maxWorkerCount);
		try {
			this.sem.acquire(maxWorkerCount - workerCount);
		}
		catch (InterruptedException e) {
			log.info("FAILED INIT WORKER POOL: {}", e.getMessage());
		}
	}

	private final Semaphore sem;
	private final ExecutorService scheduler;
	private final int minWorkerCount;
	private final int maxWorkerCount;
	private int currentWorkerCount;

	public void addWorker() {
		if (this.currentWorkerCount >= this.maxWorkerCount) {
			return;
		}
		this.sem.release(1);
		int count = ++this.currentWorkerCount;
		log.info("ADD WORKER: {}", count);
	}

	public void revokeWorker() {
		if (this.currentWorkerCount <= this.minWorkerCount) {
			return;
		}
		try {
			this.sem.acquire(1);
			int count = --this.currentWorkerCount;
			log.info("DEL WORKER: {}", count);
		}
		catch (Throwable t) {
			log.info("FAILED REVOKE WORKER: {}", t.getMessage());
		}
	}

	@Override
	public void execute(Runnable command) {
		this.scheduler.submit(() -> {
			try {
				this.sem.acquire(1);
				command.run();
			}
			catch (Throwable t) {
				log.info("WORKER EXCEPTION: {}", t.getMessage());
			}
			finally {
				this.sem.release(1);
			}
		});
	}

}

