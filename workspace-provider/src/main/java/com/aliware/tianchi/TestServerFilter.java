package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.BaseFilter;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            Result result = invoker.invoke(invocation);
            return result;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // 获取内存信息样例
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        GlobalMemory memory = hal.getMemory();

        appResponse.setAttachment("TestKey", String.valueOf(memory.getVirtualMemory().getVirtualMax()));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
