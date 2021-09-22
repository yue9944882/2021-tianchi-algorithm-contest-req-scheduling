package io.yue9944882.flowcontrol.loadbalance;

import java.util.List;

import io.yue9944882.flowcontrol.client.Bullet;
import org.apache.dubbo.rpc.Invoker;

public interface Registry {

	int indexOf(Invoker invoker);

	int count();

	void setAvg(int shard, long avgCost);

	long getAvg(int shard);
}
