package com.aliware.tianchi;

import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.cluster.support.wrapper.AbstractCluster;

public class UserCluster extends AbstractCluster {
    @Override
    protected <T> AbstractClusterInvoker<T> doJoin(Directory<T> directory) throws RpcException {
        return new UserClusterInvoker<>(directory);
    }
}
