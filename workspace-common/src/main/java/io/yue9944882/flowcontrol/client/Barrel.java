package io.yue9944882.flowcontrol.client;

import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.yue9944882.flowcontrol.basic.Response;
import io.yue9944882.flowcontrol.loadbalance.Registry;
import io.yue9944882.flowcontrol.traffic.Constants;
import io.yue9944882.flowcontrol.traffic.TrafficControlRequest;
import io.yue9944882.flowcontrol.traffic.TrafficControlResult;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Barrel implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(Barrel.class);

	public Barrel(Registry registry, String name, Semaphore semaphore, Invoker invoker, Ammo ammo) {
		this.name = name;
		this.semaphore = semaphore;
		this.invoker = invoker;
		this.ammo = ammo;
		this.registry = registry;
	}

	private final String name;
	private final Semaphore semaphore;
	private final Registry registry;
	private final Invoker invoker;
	private final Ammo ammo;
	private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);

	@Override
	public void run() {
		try {
			semaphore.acquire(1);
			OffsetDateTime start = OffsetDateTime.now();
			Invocation inv = build(invoker);
			TrafficControlRequest req = new TrafficControlRequest(0,
				registry.indexOf(invoker),
				registry.count(),
				inv);
			ammo.inject(registry.indexOf(invoker), req);
			AtomicBoolean released = new AtomicBoolean(false);
			timeoutScheduler.schedule(() -> {
				if (released.compareAndSet(false, true)) {
					semaphore.release(1);
				}
			}, 50, TimeUnit.MILLISECONDS);
			invoker.invoke(inv)
				.whenCompleteWithContext((r, t) -> {
					if (released.compareAndSet(false, true)) {
						semaphore.release(1);
					}
					OffsetDateTime end = OffsetDateTime.now();
					if (t == null) {
						long avg = 0;
						Object avgRaw = r.getObjectAttachment(Constants.RESP_HEADER_KEY_AVG_COST_MILLIS);
						if (avgRaw != null) {
							avg = (long) avgRaw;
						}
						log.info("{} API COST: {} (AVG {})", Clients.getName(invoker),
							end.toInstant().toEpochMilli() - start.toInstant().toEpochMilli(), avg);
						registry.setAvg(registry.indexOf(invoker), avg);
						TrafficControlResult.CraftResult[] craftResults = TrafficControlResult.parseResponses(r);
						for (TrafficControlResult.CraftResult result : craftResults) {
							Response resp = Gatlin.getInstance().popResponse(result.getSeq());
							if (resp != null) {
								resp.complete(result);
								Gatlin.getInstance().getWindow().finish(result.getSeq(), "DONE BY " + name);
							}
							else {
								log.info("NO SUCH RESPONSE {} by {}", result.getSeq(), Clients.getName(invoker));
							}
						}
					}
					else {
						if (t instanceof CompletionException && t
							.getCause() instanceof RemotingException) {
							RemotingException e = (RemotingException) t.getCause();
							if (e.getMessage().contains("thread pool is exhausted")) {
								log.info("EXHAUST {}", Clients.getName(invoker));
							}
						}
					}
				});
		}
		catch (Throwable t) {
			log.info("BARREL {} ABORT: {}", name, t.getMessage());
		}
		finally {
		}
	}


	public static RpcInvocation build(Invoker invoker) {
		RpcInvocation i = new RpcInvocation();
		i.setInvoker(invoker);
		i.setArguments(new Object[] {"kimmin"});
		i.setServiceName("com.aliware.tianchi.HashInterface");
		i.setMethodName("hash");
		i.setTargetServiceUniqueName("com.aliware.tianchi.HashInterface");
		i.setParameterTypes(new Class[] {String.class});
		i.setReturnType(CompletableFuture.class);
		i.setParameterTypesDesc("Ljava/lang/String;");
		return i;
	}

}
