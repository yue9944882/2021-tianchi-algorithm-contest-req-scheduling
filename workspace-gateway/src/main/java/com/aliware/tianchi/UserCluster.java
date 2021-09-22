package com.aliware.tianchi;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.yue9944882.flowcontrol.basic.Request;
import io.yue9944882.flowcontrol.basic.Response;
import io.yue9944882.flowcontrol.basic.RpcRequest;
import io.yue9944882.flowcontrol.basic.RpcResponse;
import io.yue9944882.flowcontrol.client.Barrel;
import io.yue9944882.flowcontrol.client.Gatlin;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.cluster.support.wrapper.AbstractCluster;

public class UserCluster extends AbstractCluster {
	@Override
	protected <T> AbstractClusterInvoker<T> doJoin(Directory<T> directory) throws RpcException {
		// TODO: warm up
		List<Invoker> invokers = directory.getAllInvokers().stream().collect(Collectors.toList());
		Gatlin.init(invokers);
		try {
			for (Invoker invoker : invokers) {
				RpcInvocation invocation = Barrel.build(invoker);
				invoker.invoke(invocation);
			}
			for (Invoker invoker : invokers) {
				Request req = Gatlin.getInstance().getWindow().start(Barrel.build(invoker), OffsetDateTime.now());
				Response resp = new RpcResponse(OffsetDateTime.now(), req);
				Gatlin.getInstance().schedule(req.getSeq(), resp);
				Gatlin.getInstance().getWindow().finish(req.getSeq(), "DONE");
			}
		}
		catch (Throwable t) {
			t.printStackTrace();
		}
		return new UserClusterInvoker<>(directory);
	}

}
