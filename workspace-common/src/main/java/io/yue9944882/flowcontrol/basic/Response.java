package io.yue9944882.flowcontrol.basic;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.dubbo.rpc.Result;

public interface Response {

	int getSeq();

	void complete(Result result);

	void complete(Throwable t);
}
