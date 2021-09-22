package io.yue9944882.flowcontrol.basic;

import org.apache.dubbo.rpc.Invocation;

public interface Request {

	int getSeq();

	String getInput();

	Invocation getInvocation();

}
