package com.aliware.tianchi;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.dubbo.rpc.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(Worker.class);

	public Worker(
		String name,
		Map<Integer, Boolean> cache,
		AtomicInteger dynamicLeft,
		WorkQueue inputQueue,
		BlockingQueue<DigestWithResponse> outputQueue) {
		this.name = name;
		this.dynamicLeft = dynamicLeft;
		this.cache = cache;
		this.inputQueue = inputQueue;
		this.outputQueue = outputQueue;
	}


	private final String name;
	private final AtomicInteger dynamicLeft;
	private final Map<Integer, Boolean> cache;
	private final WorkQueue inputQueue;
	private final BlockingQueue<DigestWithResponse> outputQueue;

	@Override
	public void run() {
		while (true) {
			try {
				DigestWithInvocation input = inputQueue.pop();
				if (input == null) {
					Thread.sleep(2L);
					continue;
				}
				if (this.dynamicLeft.get() > input.getSeq()) {
					continue;
				}

				if (input.getPriority() != DigestWithInvocation.PRIORITY_MANDATORY) {
					if (Boolean.TRUE.equals(this.cache.get(input.getSeq()))) {
						continue;
					}
				}
				if (Boolean.TRUE.equals(this.cache.containsKey(input.getSeq()))) {
					continue;
				}
				this.cache.put(input.getSeq(), true);
				long start = System.currentTimeMillis();
				try {
					Result result = input.getInvoker().invoke(input.getInvocation()).get();
					DigestWithResponse output = new DigestWithResponse(result, input.getSeq(), input.getInput());
					if (this.outputQueue.offer(output)) {
						log.info("WORKER {} OFFER: {} COST {}",
							name, output.getSeq(), System.currentTimeMillis() - start);
					}
				}
				catch (InterruptedException | ExecutionException e) {
					log.info("WORKER {} OFFER FAIL: {}: {}", name, input.getSeq(), e.getMessage());
				}
			}
			catch (Throwable t) {
				log.info("WORKER {} ABORT: {}", name, t.getMessage());
			}
		}
	}
}
