package io.yue9944882.flowcontrol.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.yue9944882.flowcontrol.basic.Response;
import io.yue9944882.flowcontrol.loadbalance.Registry;
import io.yue9944882.flowcontrol.loadbalance.InvokerRegistry;
import io.yue9944882.flowcontrol.prober.FixedProber;
import io.yue9944882.flowcontrol.prober.Prober;
import io.yue9944882.flowcontrol.window.Window;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gatlin {

	private static final Logger log = LoggerFactory.getLogger(Gatlin.class);
	private static Gatlin instance;
	private static boolean init = false;

	public static Gatlin getInstance() {
		return instance;
	}

	public static void init(List<Invoker> initInvokers) {
		if (!init) {
			synchronized (Gatlin.class) {
				if (!init) {
					instance = new Gatlin(initInvokers, new Window(), new InvokerRegistry(initInvokers), 80);
					instance.start();
					init = true;
				}
			}
		}
	}

	private Gatlin(List<Invoker> invokers, Window window, Registry registry, int poolCount) {
		Prober prober = new FixedProber();
		this.pool = Executors.newFixedThreadPool(poolCount);
		this.poolCount = poolCount;
		this.window = window;
		this.registry = registry;
		this.barrels = new HashMap<>();
		this.engines = new HashMap<>();
		this.ammo = new Ammo(this.window, this.registry, prober);
		for (Invoker invoker : invokers) {
			Semaphore sem = new Semaphore(poolCount, true);
			String name = Clients.getName(invoker);
			this.engines.put(name, Executors.newScheduledThreadPool(poolCount));
			this.barrels.put(name, new Barrel(registry, prober, name, sem, invoker, ammo));
		}
	}

	private final Ammo ammo;
	private final Map<String, Barrel> barrels;
	private final Map<Integer, Response> pending = new ConcurrentHashMap<>();
	private final Map<String, ScheduledExecutorService> engines;
	private final ExecutorService pool;
	private final int poolCount;
	private final Window window;
	private final Registry registry;
	private boolean started = false;

	public void start() {
		if (!started) {
			synchronized (this) {
				if (!started) {
					for (Map.Entry<String, Barrel> e : this.barrels.entrySet()) {
						String name = e.getKey();
						Barrel barrel = barrels.get(name);
						ScheduledExecutorService workers = this.engines.get(name);
						workers.scheduleWithFixedDelay(
							barrel,
							0,
							1,
							TimeUnit.MILLISECONDS);
					}
					this.ammo.start();
				}
			}
		}
	}

	public Window getWindow() {
		return window;
	}

	public void schedule(int seq, Response response) {
		this.pending.put(seq, response);
	}

	public Response popResponse(int seq) {
		return this.pending.remove(seq);
	}

}
