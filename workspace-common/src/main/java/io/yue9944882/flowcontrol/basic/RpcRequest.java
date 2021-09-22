package io.yue9944882.flowcontrol.basic;

import io.yue9944882.flowcontrol.traffic.Constants;
import org.apache.dubbo.rpc.Invocation;

public class RpcRequest implements Request {

	public RpcRequest(int seq, Invocation invocation) {
		this.invocation = invocation;
		this.seq = seq;
		this.invocation.setAttachment(Constants.REQ_HEADER_KEY_SEQ, seq);
	}

	private final int seq;
	private final Invocation invocation;

	@Override
	public String getInput() {
		return (String) this.invocation.getArguments()[0];
	}

	@Override
	public Invocation getInvocation() {
		return this.invocation;
	}

	@Override
	public int getSeq() {
		return this.seq;
	}
}
