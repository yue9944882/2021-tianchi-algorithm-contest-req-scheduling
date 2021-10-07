package com.aliware.tianchi;

import io.yue9944882.flowcontrol.window.Digest;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

public class DigestWithInvocation extends Digest {

	public DigestWithInvocation(int priority, int seq, String input, Invoker invoker, Invocation invocation) {
		super(seq, input);
		this.priority = priority;
		this.invoker = invoker;
		this.invocation = invocation;
	}

	private final Invocation invocation;
	private final Invoker invoker;
	private int priority;

	public Invocation getInvocation() {
		return invocation;
	}

	public Invoker getInvoker() {
		return invoker;
	}

	public int getPriority() {
		return priority;
	}

	public DigestWithInvocation setPriority(int priority) {
		this.priority = priority;
		return this;
	}
}
