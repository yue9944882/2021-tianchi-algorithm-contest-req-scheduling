package io.yue9944882.flowcontrol.client;

import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.List;

import io.yue9944882.flowcontrol.basic.Request;
import io.yue9944882.flowcontrol.basic.Response;
import io.yue9944882.flowcontrol.basic.RpcRequest;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;

public class Bullet {
	public Bullet(List<Invoker> invokers, Request request, Response response, int retry, OffsetDateTime arriveTime) {
		this.request = request;
		this.invokers = invokers;
		this.response = response;
		this.retry = retry;
		this.arriveTime = arriveTime;
	}

	private final Request request;
	private final List<Invoker> invokers;
	private final Response response;
	private final int retry;
	private final OffsetDateTime arriveTime;

	public List<Invoker> getInvokers() {
		return invokers;
	}

	public Request getRequest() {
		return request;
	}

	public Response getResponse() {
		return response;
	}

	public int getRetry() {
		return retry;
	}

	public OffsetDateTime getArriveTime() {
		return arriveTime;
	}

	public Bullet next() {
		return new Bullet(invokers, request, response, retry + 1, arriveTime);
	}

	public static class Comparator implements java.util.Comparator<Bullet> {
		@Override
		public int compare(Bullet o1, Bullet o2) {
			int retryCmp = Integer.compare(o2.getRetry(), o1.getRetry());
			return retryCmp;
		}
	}
}
