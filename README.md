[README in English](README-EN.md)
# WPaxos

WPaxos是Paxos一致性算法的生产级高性能Java实现，参考了微信团队C++语言开发的[PhxPaxos](https://github.com/Tencent/phxpaxos)类库，支持多分组，可用于解决高并发、高可靠分布式系统中多副本数据一致性问题以及分布式共识问题。针对一些网络分区、机器宕机、进程异常（OOM、卡顿、强制关闭）等突发情况，已经过一系列实际应用场景的验证。

## 功能特性  
- 高性能：Multi-Paxos算法与Basic-Paxos算法结合，支持多Paxos分组，有序确定多个值  
- 节点间可通过状态机checkpoint或逐条数据流两种方式对落后数据快速对齐  
- 具有网络分区容错性，集群少数节点故障服务高可用性  
- 提供有Master自动选举功能  
- 集群可通过Paxos协议动态、安全的增加节点、删除节点  
- 高扩展性：支持存储模块与异步通信模块自定义  
- 一个Paxos实例可以同时挂载多个状态机  
- 提交的数据支持增量checksum校验  
- 可添加不参与提案投票，仅用于备份数据的follower节点  
- 默认存储支持按照时间与holdcount两种清理paxoslog方式  


## 需要
编译需要 JDK 8 及以上。

## 性能
#### 测试运行环境
CPU：20 x Intel(R) Xeon(R) Silver 4114 CPU @ 2.20GHz  
内存：192 GB  
硬盘：ssd  
网卡：万兆网卡  
集群机器个数： 3个  

#### 测试结果
采用源码中Simple demo为测试样例，为了排除其它干扰，状态机空跑；instance索引采用默认的leveldb方式；直接在服务节点本地调propose接口测试，
group数量大于等于3时，master均匀分布于集群3个节点，每个group由master节点发起propose。
  
**场景一：无批量提交**  

|  data-size  |  group-count  |  qps  |  data-size  |  group-count  |  qps  |  data-size  |  group-count  |  qps  
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| 100B | 1 | 3835 | 2KB | 1 | 2671 | 100KB | 1 | 337 |
| 100B | 20 | 29577 | 2KB | 20 | 20259 | 100KB | 20 | 1416 |
| 100B | 50 | 34165 | 2KB | 50 | 22053 | 100KB | 50 | 1350 |
| 100B | 100 | 24495 | 2KB | 100 | 17131 | 100KB | 100 | 1300 |


**场景二：批量提交**  

不同size数据在批量提交时，每个group达到最大性能的最佳batch取值及索引方式也有所不同，下面是部分测试数据。

| data-size | group-count | batch-count | 索引方式 | qps(W) |
| ------ | ------ | ----- | ------ | ------ |
| 100B | 1 | 550 | 文件索引 | 17 |
| 100B | 20 | 250 | 文件索引 | 137 |
| 100B | 50	| 150 | 文件索引 | 132 |
| 100B | 100 | 150 | levelDB索引 | 75 |
| 2KB | 1 | 250	| 文件索引 | 1 |
| 2KB | 20 | 20	| 文件索引 | 17 |
| 2KB | 50 | 10	| 文件索引 | 22 |
| 2KB | 100 | 20 | levelDB索引 | 16 |  

**场景三：leveldb索引与文件索引对比**  

下面是size100B数据非批量提交时，不同group下Leveldb与文件两种索引方式的性能对比，从结果分析，在group数量比较小时，文件索引性能要优于leveldb索引。  

| data-size | group-count | 文件索引qps | levelDB索引qps |
| ------ | ------ | ----- | ------ |
| 100B | 1 | 4207 | 3835 |
| 100B | 3 | 11595 | 9684 |
| 100B | 9	| 25035 | 21027 |
| 100B | 18 | 35277 | 29577 |
| 100B | 36 | 40062	| 34398 |
| 100B | 54 | 44061	| 36495 |
| 100B | 72 | 40929	| 54345 |
| 100B | 100 | 37830 | 42015 |  

## 快速使用
下面以sample中的echo为例，说明如何快速使用WPaxos，构建WPaxos集群有以下三个关键步骤：构建状态机、构建上下文信息和初始化WPaxos实例。
#### 创建状态机
首先，状态机为WPaxos数据的最终处理逻辑，instance数据一旦被状态机执行，不会再被修改，集群中所有节点只要初始状态一致，那么状态机最终的状态也保持一致。这里我们实现一个状态机叫EchoSM，该类继承StateMachine，如下： 
```
public class EchoSM implements StateMachine {
	public static final int SMID = 1;·
	private int groupId;
	public EchoSM(int i) {
		this.groupId = i;
	}
	@Override
	public int getSMID() {
		return SMID;
	}
	@Override
	public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
		System.out.println("[SM Execute] ok, smid " + this.getSMID() + " instanceid " + instanceID + " value " + new String(paxosValue));
		//only commiter node have SMCtx.
		if(smCtx != null && smCtx.getpCtx() != null) {
			EchoSMCtx peCtx = (EchoSMCtx)smCtx.getpCtx();
			peCtx.setExecuteRet(0);
			peCtx.setEchoRespValue(paxosValue);
		}
		executeForCheckpoint(groupIdx, instanceID, paxosValue);
		return true;
	}

}

```
在一个Paxos实例分组中可以同时挂载多个状态机，但是同一instance数据只会选择应用于某一个状态机，每个状态机采用唯一的SMID来标识。
execute()方法为状态机的处理逻辑，其中  
第一个参数为groupIdx，用来区分不同Paxos分组实例；  
第二个参数为instanceID，为一个全局递增的序列号，标识每次提交数据执行的编号；  
第三个为paxosValue，即我们提交的数据；  
第四个参数为SMCtx类的实例，即整个执行过程的上下文信息，下文中介绍。  
WPaxos会保证同一个Paxos group的多个节点都会执行相同序列的状态机execute()方法，从而实现强一致性。  

#### 构建上下文信息
既然每条提交的数据都可选择应用的状态机，那么就需要一个上下文信息，用来保存数据应用的目标状态机信息。
这里用到了前文说的SMCtx类，如下：
```
public class SMCtx {
	private int smId;
	private Object pCtx;
	
	public SMCtx() {
	}
	
	public SMCtx(int smId, Object pCtx) {
		super();
		this.smId = smId;
		this.pCtx = pCtx;
	}
}

```
SMCtx类保存了整个执行过程的上下文信息，由写入者初始化，并且会传入到状态机的execute()方法，主要封装有以下两种信息：   
smId，即目标执行状态机ID；  
pCtx，保存了用户自定义的上下文数据，用户在状态机中使用该变量，可用于回传状态机执行结果。  

下面为echo上下文的定义：
```
public class EchoSMCtx {
	/*状态机执行是否成功*/
	public int executeRet;
	/*状态机执行数据*/
	private byte[] echoRespValue;
}
```
以上两个参数都是用来回传状态机执行结果信息。  


#### 运行WPaxos实例
在编写好echo状态机之后，接下来就是运行WPaxos，并且挂载echo状态机。
如下，是EchoServer类，用于封装Paxos node初始化、启动或其它接口调用：
```
public class EchoServer {
	private NodeInfo myNode;
    	private List<NodeInfo> nodeList;
    	private String rootPath;
    	private Node paxosNode;
    	private IndexType indexType;
    	private int groupCount;

	public EchoServer(NodeInfo myNode, List<NodeInfo> nodeList, int groupCount, String rootPath, int indexType) {
		this.myNode = myNode;
		this.nodeList = nodeList;
		this.paxosNode = null;
		this.groupCount = groupCount;
		this.rootPath = rootPath;
		if (indexType == IndexType.LEVEL_DB.getType()) {
			this.indexType = IndexType.LEVEL_DB;
		} else {
			this.indexType = IndexType.PHYSIC_FILE;
		}
	}
	
	public void runPaxos() throws Exception {
		Options options = new Options();
		String logStoragePath = this.makeLogStoragePath(this.rootPath);
		options.setLogStoragePath(logStoragePath);
		options.setGroupCount(groupCount);
		options.setMyNode(this.myNode);
		options.setNodeInfoList(this.nodeList);
		options.setUseMembership(true);
		options.setUseBatchPropose(false);
		options.setIndexType(indexType);
		options.setStoreConfig(new StoreConfig(rootPath, null));
		
		for(int gid = 0; gid < groupCount; gid++) {
			GroupSMInfo smInfo = new GroupSMInfo();
			smInfo.setUseMaster(true);
			smInfo.setGroupIdx(gid);
			smInfo.getSmList().add(new EchoSM(gid));
			options.getGroupSMInfoList().add(smInfo);
		}
		
		this.paxosNode = Node.runNode(options);
	}
	
}
```
先看下几个参数的意义，  
myNode标识本机的ip和port，nodeList标识Paxos集群所有节点的ip和port；  
indexType标识要使用的索引存储类型，WPaxos中提供两种索引存储类型：levelDB和文件存储。使用方可以根据自身场景决定使用哪种索引存储；  
groupCount标识要同时运行多少个Paxos分组实例，多实例之间完全独立；  
runPaxos()方法来初始化并启动WPaxos，参数Options类包含了WPaoxs运行的所有参数设置；    
设置好所有运行参数后，接下来需要挂载状态机。  
GroupSMInfo类代表一个Paxos实例的状态机列表，其中groupIdx标识Paxos分组实例ID，smList标识挂载的状态机列表。  
最后调用runNode()方法，启动WPaxos。  

#### 发起请求

我们通过echo()方法，来发起请求
```
public String echo(String echoReqValue, int groupIdx) throws Exception {
	SMCtx ctx = new SMCtx();
	EchoSMCtx echoSMctx = new EchoSMCtx();
	ctx.setSmId(EchoSM.SMID);
	ctx.setpCtx(echoSMctx);

	this.paxosNode.setTimeoutMs(3000);
	ProposeResult proposeResult = null;
	proposeResult = this.paxosNode.propose(groupIdx, echoReqValue.getBytes(), ctx);

	if (PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet() == proposeResult.getResult() && echoSMctx.getEchoRespValue() != null) {
		return new String(echoSMctx.getEchoRespValue());
	}
		
	return null;
}

```
首先定义上下文类型EchoSMCtx，然后设置到SMCtx的pCtx变量中，同时设置smID=1，标识请求数据要由smID=1的状态机来执行。
然后调用paxosNode的propose方法，发起数据同步请求，第一个参数为groupIdx，指定要执行的Paxos group，第二个参数为实际发送的数据，第三个参数为我们创建的上下文信息。  
在略复杂的应用场景，如果同一个Paxos group有多个线程并发发起propose请求同步数据，可改为调用batchPropose接口，后端异步线程会进行批量合并，可提高整体吞吐量。  

通过以上几个步骤，我们就利用WPaxos实现了一个简单的多机的echo()方法。

#### 运行结果

我们启动三副本的节点，第一台机器：
```
echo server start, ip [127.0.0.1] port [30000]
please input : 
1
[SM Execute] ok, smid 1 instanceid 5 value wpaxos_test0
echo response : wpaxos_test0
```

ip为127.0.0.1，port为30000的节点，可以看到我们输入的paxosValue为"wpaxos_test0"，execute执行的结果打印出来的成功结果ok，通过上下文获取的返回值为"wpaxos_test0"，与我们的输入相同。

看下其他副本的情况：
```
echo server start, ip [127.0.0.1] port [30001]
please input : 
[SM Execute] ok, smid 1 instanceid 5 value wpaxos_test0
```
ip为127.0.0.1，port为30001的机器，execute执行结果与第一台机器相同。同样第三台机器执行结果也相同。

#### Master功能
Master是WPaxos中的一个角色，在多机构建的集合里面，任意时刻，每个Paxos实例中，最多只有一个节点能够成为master。  
Master是一个十分实用的功能，在多数的分布式系统中，一般会选择master节点来同步更新数据到其它节点。  

下面展示如何使用Master功能。
首先构建一个选举类：
```
public class ElectionServer {
    	private NodeInfo myNode;
    	private List<NodeInfo> nodeList;
    	private String rootPath;
    	private Node paxosNode;
    	private int groupCount;

	public ElectionServer(NodeInfo myNode, List<NodeInfo> nodeList, int groupCount, String rootPath) {
		this.myNode = myNode;
		this.nodeList = nodeList;
		this.paxosNode = null;
		this.groupCount = groupCount;
		this.rootPath = rootPath;
	}
	
	public void runPaxos() throws Exception {
		Options options = new Options();
		String logStoragePath = this.makeLogStoragePath(this.rootPath);
		options.setLogStoragePath(logStoragePath);
		options.setGroupCount(groupCount);
		options.setMyNode(this.myNode);
		options.setNodeInfoList(this.nodeList);
		options.setUseMembership(true);
		options.setUseBatchPropose(false);
		options.setStoreConfig(new StoreConfig(rootPath, null));
		
		for(int gid = 0; gid < groupCount; gid++) {
			GroupSMInfo smInfo = new GroupSMInfo();
			smInfo.setUseMaster(true);
			smInfo.setGroupIdx(gid);
			options.getGroupSMInfoList().add(smInfo);
		}
		
		this.paxosNode = Node.runNode(options);
	}
	
	public NodeInfo getMasterNode(int groupIdx) {
		return this.paxosNode.getMaster(groupIdx);
	}
	
	public boolean isMaster(int groupIdx) {
		return this.paxosNode.isIMMaster(groupIdx);
	}

}
```
方法getMasterNode()获取当前实例的master节点信息。  
isMaster()方法返回当前节点是否是master。  
runPaxos()方法启动Paxos实例，这里看到与echo不同在于，不需要单独实现状态机，而是通过将GroupSMInfo的useMaster设置为true，开启内置的MasterStateMachine状态机。  

这样我们就实现了master的选举。  

#### 成员变化
在Paxos实例运行过程中，可以动态增加或者删除集群成员，同样也不需要单独构建状态机，而是利用内置的SystemVSM状态机实现。  
调用paxosNode的增加成员、删除成员方法即可，如下：
```
paxosNode.addMember(groupIdx, node);
paxosNode.removeMember(groupIdx, node);
```
#### 其它
以上为简单的echo样例实现介绍，一些高级应用如数据快照checkpoint相关的使用，由于要结合不同应用场景实现，这里不再详细介绍，有需要可以参考KV存储样例中相关实现。  

## 文档
[DUCUMENT](DOCUMENT.md)

## 如何贡献
[CONTRIBUTING](CONTRIBUTING.md)

## 致谢
感谢腾讯微信团队开源了如此优秀的 C++ [PhxPaxos](https://github.com/Tencent/phxpaxos)实现；
感谢[Apache RocketMQ](https://github.com/apache/rocketmq)团队开源的关于文件管理部分的高效灵活设计；

## 开源许可
WPaxos 基于 [Apache License 2.0](./LICENSE) 开源协议，源码中引用了开源协议同为 Apache License 2.0 的[Apache RocketMQ](https://github.com/apache/rocketmq/tree/master/store)存储部分关于文件管理相关的代码。

## 联系我们  
<img src="img/wpaxos-wechat.jpg"/>  
欢迎添加wpaxos助手微信账号，加入wpaxos技术讨论群~


