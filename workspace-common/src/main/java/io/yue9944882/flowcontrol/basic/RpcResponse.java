package io.yue9944882.flowcontrol.basic;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.dubbo.rpc.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class RpcResponse implements Response, Result {

	private static final Logger log = LoggerFactory.getLogger(RpcResponse.class);

	public RpcResponse(OffsetDateTime startTimestamp, Request request) {
		this.startTimestamp = startTimestamp;
		this.request = request;
	}

	private final OffsetDateTime startTimestamp;
	private final Request request;
	private final List<BiConsumer<Result, Throwable>> whenCompletes = new ArrayList<>();
	private final CompletableFuture<Result> future = new CompletableFuture<>();
	private final CompletableFuture<Object> returningFuture = CompletableFuture.anyOf(future).thenApply(o -> {
		try {
			return ((Result) o).recreate();
		}
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
	});
	private final AtomicBoolean completed = new AtomicBoolean(false);


	@Override
	public int getSeq() {
		return this.request.getSeq();
	}

	@Override
	public void complete(Result result) {
		if (!completed.compareAndSet(false, true)) {
			return;
		}
		if (result == null) {
			return;
		}
		if (future.isDone()) {
			return;
		}
		try {
			this.future.complete(result);
		}
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
		this.whenCompletes.stream().forEach(w -> w.accept(result, null));
	}

	@Override
	public void complete(Throwable t) {
		if (OffsetDateTime.now().isAfter(startTimestamp.plus(Duration.ofSeconds(5)))) {
			this.completed.set(true);
			this.whenCompletes.stream().forEach(w -> w.accept(null, t));
		}
		log.error("", t);
	}

	@Override
	public Result whenCompleteWithContext(BiConsumer<Result, Throwable> fn) {
		this.whenCompletes.add(fn);
		return this;
	}

	@Override
	public Object recreate() throws Throwable {
		return returningFuture;
	}

	@Override
	public Object getValue() {
		throw new NotImplementedException();
	}

	@Override
	public void setValue(Object value) {
		throw new NotImplementedException();
	}

	@Override
	public Throwable getException() {
		throw new NotImplementedException();
	}

	@Override
	public void setException(Throwable t) {
		throw new NotImplementedException();
	}

	@Override
	public boolean hasException() {
		throw new NotImplementedException();
	}

	@Override
	public Map<String, String> getAttachments() {
		throw new NotImplementedException();
	}

	@Override
	public Map<String, Object> getObjectAttachments() {
		throw new NotImplementedException();
	}

	@Override
	public void addAttachments(Map<String, String> map) {
		throw new NotImplementedException();
	}

	@Override
	public void addObjectAttachments(Map<String, Object> map) {
		throw new NotImplementedException();
	}

	@Override
	public void setAttachments(Map<String, String> map) {
		throw new NotImplementedException();
	}

	@Override
	public void setObjectAttachments(Map<String, Object> map) {
		throw new NotImplementedException();
	}

	@Override
	public String getAttachment(String key) {
		throw new NotImplementedException();
	}

	@Override
	public Object getObjectAttachment(String key) {
		throw new NotImplementedException();
	}

	@Override
	public String getAttachment(String key, String defaultValue) {
		throw new NotImplementedException();
	}

	@Override
	public Object getObjectAttachment(String key, Object defaultValue) {
		throw new NotImplementedException();
	}

	@Override
	public void setAttachment(String key, String value) {
		throw new NotImplementedException();
	}

	@Override
	public void setAttachment(String key, Object value) {
		throw new NotImplementedException();
	}

	@Override
	public void setObjectAttachment(String key, Object value) {
		throw new NotImplementedException();
	}


	@Override
	public <U> CompletableFuture<U> thenApply(Function<Result, ? extends U> fn) {
		throw new NotImplementedException();
	}

	@Override
	public Result get() throws InterruptedException, ExecutionException {
		throw new NotImplementedException();
	}

	@Override
	public Result get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		throw new NotImplementedException();
	}

}
