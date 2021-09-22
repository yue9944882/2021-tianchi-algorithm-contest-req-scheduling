package io.yue9944882.flowcontrol.client;

import org.apache.dubbo.rpc.Invoker;

public class Clients {

	public static String getName(Invoker invoker) {
		return invoker.getUrl().getAddress();
	}

}
