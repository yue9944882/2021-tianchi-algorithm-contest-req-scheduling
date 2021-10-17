package com.aliware.tianchi;

import java.time.OffsetDateTime;
import java.util.List;

import io.yue9944882.flowcontrol.basic.Request;
import io.yue9944882.flowcontrol.basic.RpcResponse;
import io.yue9944882.flowcontrol.client.Gatlin;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群实现
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的集群调度算法
 */
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {

	private static final Logger log = LoggerFactory.getLogger(UserClusterInvoker.class);

	public UserClusterInvoker(Directory<T> directory) {
		super(directory);
	}

	@Override
	protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
		OffsetDateTime start = OffsetDateTime.now();
		Request req = Gatlin.getInstance().getWindow().start(invocation);
		RpcResponse response = new RpcResponse(start, req);
		Gatlin.getInstance().schedule(req.getSeq(), response);
		return response;
	}
}
