package com.aliware.tianchi;

import io.yue9944882.flowcontrol.window.Digest;
import org.apache.dubbo.rpc.Result;

public class DigestWithResponse extends Digest {

	public DigestWithResponse(Result result, int seq, String input) {
		super(seq, input);
		this.result = result;
	}

	public Result getResult() {
		return result;
	}

	private final Result result;

}
