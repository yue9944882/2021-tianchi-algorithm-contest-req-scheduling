package com.aliware.tianchi;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.yue9944882.flowcontrol.param.Parameters;
import io.yue9944882.flowcontrol.traffic.Constants;
import io.yue9944882.flowcontrol.traffic.TrafficControlRequest;
import io.yue9944882.flowcontrol.traffic.TrafficControlResult;
import io.yue9944882.flowcontrol.window.Digest;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.LRUCache;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.BaseFilter;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {
	public TestServerFilter() {
	}

	private static final Logger log = LoggerFactory.getLogger(TestServerFilter.class);
	private static final int maxProcessRecall = Parameters.PROVIDER_MAX_PROCESS_RECALL;
	private static final long timeoutMillis = Parameters.PROVIDER_PROCESS_TOTAL_TIMEOUT_MILLIS;
	private static final long recallTimeoutMillis = Parameters.PROVIDER_PROCESS_RECALL_TIMEOUT_MILLIS;

	private final LRUCache<Integer, Boolean> lru = new LRUCache<>(Parameters.PROVIDER_LRU_CACHE_CAPACITY);
	private final BlockingQueue<DigestWithResponse> outputQueue = new LinkedBlockingQueue<>();
	private final WorkerPool pool = new WorkerPool(
		Parameters.PROVIDER_PID_CONCURRENCY_MIN,
		Parameters.PROVIDER_PID_CONCURRENCY_MAX,
		outputQueue);
	private final Controller autoscaler = new Controller(pool);
//	private final ScheduledExecutorService retry = Executors.newScheduledThreadPool(10);
//	private final long retryMillis = 300L;

	@Override
	public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
		try {
			int[] index = TrafficControlRequest.getIndex(invocation);
			int seq = TrafficControlRequest.getSeq(invocation);
			Set<Integer> dones = new HashSet<>();
			if (seq == -1) {
				AppResponse resp = new AppResponse();
				resp.setValue(0);
				return new AsyncRpcResult(CompletableFuture.completedFuture(resp), invocation);
			}
			// parent
			long start = System.currentTimeMillis();
			AppResponse resp = new AppResponse();
			resp.setValue(0);
			AsyncRpcResult result = new AsyncRpcResult(CompletableFuture.completedFuture(resp), invocation);
			TrafficControlResult tcResult = new TrafficControlResult(result);

			// recall
			AtomicInteger recallCount = new AtomicInteger(0);
			long startRecall = System.currentTimeMillis();
			for (int i = index.length - 1; i >= 0; i--) {
				int recallIdx = index[i];
				if (Boolean.TRUE.equals(this.lru.get(recallIdx))) {
					continue;
				}
				if (recallCount.get() >= maxProcessRecall) {
					break;
				}
				long now = System.currentTimeMillis();
				if (now > startRecall + recallTimeoutMillis) {
					break;
				}
				if (now > start + timeoutMillis) {
					break;
				}
				doInvoke(invoker, invocation, recallIdx, recallCount, dones);
			}

			List<DigestWithResponse> doneRecalls = new LinkedList<>();
			outputQueue.drainTo(doneRecalls, Parameters.PROVIDER_MAX_DEQUEUE_RECALL);
			doneRecalls.stream().forEach(digestWithResponse -> {
				tcResult.append(
					digestWithResponse.getSeq(),
					digestWithResponse.getInput(),
					digestWithResponse.getResult());
			});


			long avg = autoscaler.avg();
			result.setObjectAttachment(Constants.RESP_HEADER_KEY_AVG_COST_MILLIS, avg);

			return result;
		}
		catch (Throwable t) {
			log.info("ABORT {}", t.getMessage());
			t.printStackTrace();
			throw new RpcException(t);
		}
	}

	private void doInvoke(Invoker invoker, Invocation invocation, int idx, AtomicInteger count, Set<Integer> dones) {
		Digest d = TrafficControlRequest.getRecall(invocation, idx);
		ChildInvocation i = new ChildInvocation(invocation, invoker, d);
		boolean scheduled = scheduledInvoke(invoker, i, d.getSeq(), d.getInput());
		if (scheduled) {
			count.getAndIncrement();
			dones.add(d.getSeq());
		}
	}

	private boolean scheduledInvoke(Invoker<?> invoker, Invocation invocation, int seq, String input) throws RpcException {
		Boolean prev = this.lru.put(seq, true);
		if (prev == null || !prev) {
			pool.schedule(seq, input, () -> {
				long start = System.nanoTime();
				try {
					return invoker.invoke(invocation);
				}
				finally {
					this.autoscaler.record((System.nanoTime() - start) / 1000);
				}
			});
			return true;
		}
		return false;
	}

	@Override
	public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
		// 获取内存信息样例
	}

	@Override
	public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

	}

	private class ChildInvocation extends RpcInvocation {
		ChildInvocation(Invocation invocation, Invoker invoker, Digest digest) {
			super(invocation, invoker);
			this.setAttachment(Constants.REQ_HEADER_KEY_SEQ, digest.getSeq());
			this.setArguments(new Object[] {digest.getInput()});
		}
	}

}
