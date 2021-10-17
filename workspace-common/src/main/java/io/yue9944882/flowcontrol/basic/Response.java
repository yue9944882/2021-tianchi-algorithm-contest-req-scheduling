package io.yue9944882.flowcontrol.basic;

import org.apache.dubbo.rpc.Result;

public interface Response {

	int getSeq();

	void complete(Result result);

	void complete(Throwable t);
}
