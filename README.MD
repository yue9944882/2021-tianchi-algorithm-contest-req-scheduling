## 引言


云原生带来了技术标准化的重大变革，如何让应用在云上更简单地创建和运行，并具备可弹性扩展的能力，是所有云原生基础组件的核心目标。借助云原生技术带来的弹性能力，应用可以在极短时间内扩容出一大批机器以支撑业务需要。
比如为了应对零点秒杀场景或者突发事件，应用本身往往需要数千甚至数万的机器数来提升性能以满足用户的需要，但是在扩容的同时也带来了诸如集群节点极多导致的节点异常频发、服务容量受多种客观因素影响导致节点服务能力不均等一系列的在云原生场景下集群大规模部署的问题。


Dubbo 期待基于一种柔性的集群调度机制来解决这些问题。这种机制主要解决的问题有两个方面，一是在节点异常的情况下，分布式服务能够保持稳定，不出现雪崩等问题；二是对于大规模的应用，能够以最佳态运行，提供较高的吞吐量和性能。


从单一服务视角看，Dubbo 期望的目标是对外提供一种压不垮的服务，即是在请求数特别高的情况下，可以通过选择性地拒绝一些的请求来保证总体业务的正确性、时效性。
从分布式视角看，要尽可能降低因为复杂的拓扑、不同节点性能不一导致总体性能的下降，柔性调度机制能够以最优的方式动态分配流量，使异构系统能够根据运行时的准确服务容量合理分配请求，从而达到性能最优。


## 题目内容
​

Apache Dubbo 作为一款可拓展性极高的 RPC 框架，支持高度自定义化的集群调度机制，本次比赛要求参赛者基于 Dubbo 提供的集群调度自定义化能力，辅以调用过滤链机制、自定义负载均衡机制等功能，设计一种柔性调度机制。
​

一般来说，集群大规模部署可能会遇到这些问题：
首先，由于网络波动或者是机器维护等客观原因，导致部分节点阶段性地不可用。
其次，得益于虚拟化机制，当今云计算的资源利用率可以大幅提高，这会带来诸如虚拟机之间相互争用宿主机资源，部分虚拟机会因此性能显著下降。
​

而集群的柔性调度正是指 Dubbo 能够从全局的角度合理分配请求，达到集群的自适应。具体来说使消费者能够快速地感知服务端节点性能的随机变化，通过调节发送往不同服务端节点的请求数比例分配变得更加合理，让 Dubbo 即使遇到集群大规模部署带来的问题，也可以提供最优的性能。


## 一、整体流程


[Apache Dubbo](http://dubbo.apache.org/zh/) |ˈdʌbəʊ| 是一款高性能、轻量级的开源Java RPC框架，它提供了三大核心能力：面向接口的远程方法调用，智能容错和负载均衡，以及服务自动注册和发现。其架构图如下所示：​
![image.png](https://img.alicdn.com/imgextra/i2/O1CN01h8xjbe1TOsYZYRzva_!!6000000002373-2-tps-1060-824.png)
本次比赛主要考察其中 `4. invoke` 调用流程


### 1.1 快速搭建开发环境


#### 项目

- reactive-cluster 选手按照题目提供的接口，实现 provider-consumer 协同的自适应负载均衡策略。

​

git clone https://code.aliyun.com/cloudnative2021/pullbased-cluster
cd pullbased-cluster
mvn clean install -Dmaven.test.skip=true
​


- [internal-service](https://code.aliyun.com/cloudnative2021/internal-service)内置服务，负责加载选手实现的负载均衡算法，启动 Consumer 和 Provider 程序。已经由赛题官方提供，开发过程不需要修改，只需要安装依赖。

​

git clone https://code.aliyun.com/cloudnative2021/internal-service
cd internal-service
mvn clean install -Dmaven.test.skip=true
​

#### 本地开发

- fork 本项目, clone 自己的仓库到本地
- 启动 nacos 服务端，使用 Nacos 2.0.2 版本
- 修改 internal-service 项目中的 com.aliware.tianchi.Constants 指向的 Nacos 地址到 Nacos 集群地址
- 运行 internal-service 项目中的 com.aliware.tianchi.MyProvider 启动 Provider
- 运行 internal-service 项目中的 com.aliware.tianchi.MyConsumer 启动 Consumer
- 打开浏览器 [http://localhost:8087/call](http://localhost:8087/call)，显示OK即表示配置成功。

​

#### 本地压测


在 internal-service 项目中存放了一个 wrk.lua 脚本，选手可以在该项目根目录下执行
wrk -t4 -c1024 -d90s -T5 --script=./wrk.lua --latency http://localhost:8087/invoke
进行压测，压测前请确认本机已安装压测工具 wrk


### 1.2 部署场景
![image.png](https://img.alicdn.com/imgextra/i2/O1CN01Hp8HO420vkIeZD3QP_!!6000000006912-2-tps-1224-558.png)


部署架构说明：

- 所有程序均在不同的 docker 容器中运行，每个容器都独占运行在不同的虚拟机上
- Gateway 负责将请求转发至 Provider
- Provider 处理请求返回响应
- Gateway 和 Provider 之间采用 Nacos 注册中心进行服务发现
- 选手需要设计实现 Gateway 选择 Provider 的 Cluster、LoadBalance 算法

​

测试过程：

1. PTS 作为压测请求客户端向 Gateway（Consumer） 发起 HTTP 请求，Gateway（Consumer） 加载用户实现的负载均衡算法选择一个 Provider，Provider 处理请求，返回结果。
1. 每个 Provider 的服务能力 (处理请求的速率) 都会动态变化：
    1. 三个 Provider 的每个 Provider 的处理能力会**随机变动**以模拟超售场景
    1. 三个 Provider 任意一个的处理能力**都小于**总请求量
    1. 三个 Provider 的会有**一定比例**的请求处理超时（5000ms）
    1. 三个 Provider 的每个 Provider 会**随机离线**（本次比赛不依赖 Nacos 的健康检查机制，也即是无地址更新通知）
3. 评测分为预热和正式评测两部分，预热部分不计算成绩，正式评测部分计算成绩。
3. 正式评测阶段，PTS 以固定 RPS 请求数模式向 Gateway 发送请求，1分钟后停止；
3. 以 PTS 统计的成功请求数和最大 TPS 作为排名依据。成功请求数越大，排名越靠前。成功数相同的情况下，按照最大 TPS 排名。



### 1.3 消费端请求方式


在 Dubbo 中， `Filter` 被设计用来拦截和过滤单次请求，基于这个实现，用户和开发者可以在不改变核心框架的情况下，非常方便的嵌入自己的逻辑来影响请求行为和请求数据。


从 3.0 版本开始，在保持原有 `Filter` 拦截语义的情况下，框架在消费端引入了新的拦截扩展点 `ClusterFilter`，用于在选址之前拦截请求，选手可以自行选择采用 `ClusterFilter` 或 `Filter` 进行请求拦截。
​

在 `ClusterInvoker` 中将会传入全部 Provider 信息，选手需要基于一定规则选择最佳 Provider 进行调用或者拒绝请求。


![image.png](https://img.alicdn.com/imgextra/i4/O1CN01Dt2tDC1hztBNo0Uyc_!!6000000004349-2-tps-752-175.png)


### 1.4 服务端处理方式
![image.png](https://img.alicdn.com/imgextra/i4/O1CN01V3fDZC1RfhsXlGLuF_!!6000000002139-2-tps-1574-226.png)
在服务端收到请求后会经过一系列的过滤链，最终调用到具体业务实现上。选手可以选择通过 Filter 对请求状态进行监控，亦或者是拒绝请求。
​

当服务端需要将容量信息通知消费者时，仅允许使用 `Result appResponse` 中的 attachment 进行传递，不允许对 Apache Dubbo 自有的协议体进行任何修改。
​

### 1.5 容量评估数据源


服务端容量评估方面考察的是动态对服务端自身性能信息进行评估，容量评估开发给参赛者获取的基础数据有以下这些：

- 所在机器的 CPU 信息
- 所在机器的内存信息
- 所在机器的磁盘占用信息
- 所在机器的网卡信息
- 程序所在 JVM 虚拟机状态信息
- 基于 Filter 机制的接口处理时延



基础工程已默认添加 oshi 依赖，选手可以自行通过 oshi 接口获取环境信息。
参考地址：[https://github.com/oshi/oshi](https://github.com/oshi/oshi)


## 二、实现方式


### 2.1 项目结构


- internal-service 服务的接口定义和实现，不允许修改，评测时不依赖选手编译的 jar 包；
    - 包含了服务端和客户端，服务端包含了负载动态变化的逻辑，开放给选手自己本地测试，禁止选手设计一个负载均衡算法来 hack 变化的过程；选手可以本地启动服务端客户端示例进行本地测试
- adaptive-loadbalance(workspace) 选手进行开发的模块, 评测时会以 jar 包依赖的方式加载。
    - 代码依赖 Apache Dubbo 3.0.1 版本



### 2.2 服务


Provider 是服务提供者，Gateway ( Consumer ) 是服务消费者，Gateway 消费 Provider 提供的服务。Gateway 及 Provider 服务的实现 **由赛会官方提供**。Gateway 通过 Nacos 注册中心发现 Provider 节点信息。
​

Provider 服务接口：
```java
public interface HashInterface {

  /**
   * 计算给定字符串的 hash 值
   * <li>
   *     <ol>接口的响应时间符合负指数分布 </ol>
   *     <ol>接口的并发度（允许同时调用的线程数）会随时间增加或减小，从而模拟生产环境可能的排队</ol>
   * </li>
   * @param input 要计算的字符串
   * @return 字符串的 hash 值
   */
  int hash(String input);
}

```


Consumer 在接收到客户端请求以后，会生成一个随机字符串，然后根据负载均衡算法选择一个 Provider 。 由 Provider 计算哈希值后返回，客户端会校验该哈希值与其生成的数据是否相同，如果相同则返回正常（200），否则返回异常（500）。
​

### 2.3 开发接口


#### 2.2.1 ClusterInvoker
​

必选接口，可以修改实现，不可以移动类或者修改包名，SPI 配置文件已经添加。
选手需要基于此类实现自己的集群调度算法。


```java
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {
    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        return select(loadbalance, invocation, invokers, null).invoke(invocation);
    }
}

```


#### 2.2.2 LoadBalance


必选接口，可以修改实现，不可以移动类或者修改包名，SPI 配置文件已经添加。
选手需要基于此类实现自己的负载均衡算法。
​

```java
/**
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
}

```


#### 2.2.3 ClientFilter
​

ClusterInvoker 选址后客户端过滤器，可选接口，可以修改实现，不可以移动类或者修改包名，SPI 配置文件已经添加。


```java
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try{
            Result result = invoker.invoke(invocation);
            return result;
        }catch (Exception e){
            throw e;
        }

    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {

    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}

```


#### 2.2.4 ClientClusterFilter


ClusterInvoker 选址前客户端过滤器，可选接口，可以修改实现，不可以移动类或者修改包名，SPI 配置文件已经添加。
​

```java
@Activate(group = CommonConstants.CONSUMER)
public class TestClientClusterFilter implements ClusterFilter, BaseFilter.Listener {
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

    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}

```


#### 2.2.5 ServerFilter


服务端过滤器，可选接口，可以修改实现，不可以移动类或者修改包名，SPI 配置文件已经添加。
​

```java
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try{
            Result result = invoker.invoke(invocation);
            return result;
        }catch (Exception e){
            throw e;
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {

    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}

```


### 2.4 限制


禁止参赛者的行为：

- 不允许修改任何其他 Dubbo 内部接口实现逻辑
- 不允许采用任何缓冲逻辑提高处理速度
- 不允许获取机器其他进程信息
- 不允许直接调用宿主机任何应用
- 除主办方提供的 SPI 拓展外不允许添加任何新的 SPI 实现



允许添加的 Maven 依赖项：

- org.apache.dubbo:dubbo:3.0.1
- org.slf4j:slf4j-api:1.7.26
- ch.qos.logback:logback-classic:1.2.3
- com.github.oshi:oshi-core-shaded:5.7.5



## 三、验证


### 3.1 启动和调用流程

1. 启动 Nacos 服务端
1. 启动三个 Provider 实例
1. 启动 Gateway 实例
1. 客户端通过 HTTP 访问 Gateway 服务
1. Gateway 按照选手扩展的路由和负载均衡算法选择一个 Provider 并进行调用
1. Provider 处理请求，返回结果
1. Gateway 将本次请求的结果返回至客户端(success/failure)



