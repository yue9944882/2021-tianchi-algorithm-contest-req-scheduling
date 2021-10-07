package com.aliware.tianchi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.dubbo.rpc.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerPool extends ThreadPoolExecutor {

	private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

	private static final int offset = 100;
	private static final int batch = 600;


	public WorkerPool(int workerCount, int maxWorkerCount, BlockingQueue<DigestWithResponse> outputQueue) {
		super(maxWorkerCount, maxWorkerCount,
			1000L, TimeUnit.MILLISECONDS,
			new PriorityBlockingQueue<>(5000, (r1, r2) -> {
				Task t1 = (Task) r1;
				Task t2 = (Task) r2;
				return Integer.compare((t1.seq - offset) / batch, (t2.seq - offset) / batch);

			}));
		this.outputQueue = outputQueue;
		this.minWorkerCount = workerCount;
		this.maxWorkerCount = maxWorkerCount;
		this.currentWorkerCount = workerCount;
		this.sem = new Semaphore(maxWorkerCount);
		try {
			this.sem.acquire(maxWorkerCount - workerCount);
		}
		catch (InterruptedException e) {
			log.info("FAILED INIT WORKER POOL: {}", e.getMessage());
		}
	}

	private final Semaphore sem;
	private final int minWorkerCount;
	private final int maxWorkerCount;
	private int currentWorkerCount;
	private final BlockingQueue<DigestWithResponse> outputQueue;

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
	protected void beforeExecute(Thread t, Runnable r) {
		try {
			this.sem.acquire(1);
		}
		catch (InterruptedException e) {
			log.info("SEM ABORT: {}", e.getMessage());
			e.printStackTrace();
		}
		super.beforeExecute(t, r);
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		this.sem.release(1);
		if (r instanceof Task) {
			try {
				Result result = ((Task) r).get();
				DigestWithResponse digestWithResponse = new DigestWithResponse(
					(Result) result.get(),
					((Task) r).seq,
					((Task) r).input);
				outputQueue.offer(digestWithResponse);
			}
			catch (InterruptedException | ExecutionException e) {
				log.info("POOL ABORT: {}", e.getMessage());
			}
		}
	}

	public void schedule(int seq, String input, Callable<Result> command) {
		super.execute(new Task(seq, input, command));
	}

	public static class Task extends FutureTask<Result> {
		public Task(int seq, String input, Callable<Result> callable) {
			super(callable);
			this.seq = seq;
			this.input = input;
		}

		final int seq;
		final String input;
	}
}

