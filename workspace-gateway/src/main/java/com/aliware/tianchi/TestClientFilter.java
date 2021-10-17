package com.aliware.tianchi;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import io.yue9944882.flowcontrol.client.Gatlin;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.rpc.BaseFilter;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {

	private static final Logger log = LoggerFactory.getLogger(TestClientFilter.class);

	@Override
	public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
		ExecutorRepository executorRepository =
			ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
		ExecutorService pool = executorRepository.getExecutor(invoker.getUrl());
		if (pool == null || pool.isShutdown() || pool.isTerminated()) {
			synchronized (this) {
				pool = executorRepository.getExecutor(invoker.getUrl());
				if (pool == null || pool.isShutdown() || pool.isTerminated()) {
					executorRepository.createExecutorIfAbsent(invoker.getUrl());
				}
			}
		}
		try {
			Result result = invoker.invoke(invocation);
			return result;
		}
		catch (Exception e) {
			throw e;
		}

	}

	@Override
	public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
	}

	@Override
	public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
	}
}
