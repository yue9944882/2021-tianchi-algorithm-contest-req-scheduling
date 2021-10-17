package io.yue9944882.flowcontrol.prober;

public interface Prober {

	void recordFailure(int shard);

	boolean isHealthy(int shard);
}
