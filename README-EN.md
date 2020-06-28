# WPaxos
WPaxos is a production-grade high-performance java implementation of paxos consensus algorithm, referenced to the [Phxpaxos](https://github.com/Tencent/phxpaxos) class library developed by the WeChat team in the C++ language.It supports multi-paxos-group and can be used to solve the problems of data consistency of multi replica in distributed system.In addition, it has been verified by a series of practical application scenarios for some emergent situations such as network partition, machine breakdown, process exceptions(OOM,stuck,forced shutdown,etc).

## Features
- High-performance：combined the algorithms of Multi-Paxos with Basic-Paxos,supporting mulit-paxos-group and chosing multiple values in order
- The fall behind data between the nodes can quickly catch up through state machine checkpoint or item-by-item data stream
- Strong fault tolerance of network partition, and high availability of services when a few nodes failure in the cluster
- Provides the Master automatic election mechanism
- Cluster can add or delete nodes through Paxos protocol dynamically and securely
- High scalability: supporting to customize storage module and asynchronous communication module
- A Paxos instance can mount multiple state machines at the same time
- The commited data supports incremental checksum validation
- A paxos instance can add a follower node that does not participate in the proposal voting but only used for backup data
- The default storage supports cleaning paxoslog by time or Holdcount 

## Requirements
The compiling environment requires JDK 8 or above

## Performance
#### Setup
CPU: 20 x Intel(R) Xeon(R) Silver 4114 CPU @ 2.20GHz  
Memory：192 GB  
Disk：ssd raid5  
Network: Gigabit Ethernet  
Cluster Nodes: 3  

#### Performance Test Result
Using the simple demo in the source code as the test example. In order to eliminate other distractions, the state machine runs empty.Index storage adopts the default LevelDB mode.Directly calling the propose interface locally on the server to test.When the number of paxos groups is greater than or equal to 3, master is evenly distributed among the three nodes of the cluster, and each group sends propose request only by the Master node.

**Scene 1：non batch commit**  

|  data-size  |  group-count  |  qps  |  data-size  |  group-count  |  qps  |  data-size  |  group-count  |  qps  
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| 100B | 1 | 3835 | 2KB | 1 | 2671 | 100KB | 1 | 337 |
| 100B | 20 | 29577 | 2KB | 20 | 20259 | 100KB | 20 | 1416 |
| 100B | 50 | 34165 | 2KB | 50 | 22053 | 100KB | 50 | 1350 |
| 100B | 100 | 24495 | 2KB | 100 | 17131 | 100KB | 100 | 1300 |


**Scene 2：batch commit**  
When data with different sizes are commited in batches，the optimal batch count and storage index mode for each group to achieve best performance are also different.Here are some of the test data.

| data-size | group-count | batch-count | index mode | qps(W) |
| ------ | ------ | ----- | ------ | ------ |
| 100B | 1 | 550 | file index | 17 |
| 100B | 20 | 250 | file index | 137 |
| 100B | 50	| 150 | file index | 132 |
| 100B | 100 | 150 | levelDB index | 75 |
| 2KB | 1 | 250	| file index | 1 |
| 2KB | 20 | 20	| file index | 17 |
| 2KB | 50 | 10	| file index | 22 |
| 2KB | 100 | 20 | levelDB index | 16 |  

**Scene 3：Leveldb index vs. File index**  
The following is the performance comparison between Leveldb index and file index under different groups when size100B data is commited non in batch. According to the result analysis, when group number is smaller, the performance of file index is better than that of Leveldb index.  

| data-size | group-count | File-index qps | LevelDB-index qps |
| ------ | ------ | ----- | ------ |
| 100B | 1 | 4207 | 3835 |
| 100B | 3 | 11595 | 9684 |
| 100B | 9	| 25035 | 21027 |
| 100B | 18 | 35277 | 29577 |
| 100B | 36 | 40062	| 34398 |
| 100B | 54 | 44061	| 36495 |
| 100B | 72 | 40929	| 54345 |
| 100B | 100 | 37830 | 42015 |  

## Quick Start  
We will take echo as an example to show you how to use WPaxos quickly.There are three important steps in building a WPaxos cluster: building state machines, building context, and initializing a WPaxos instance.

#### Building state machines  
The state machine is the final processing logic of WPaxos data. Once a instance commited data is executed by the state machine, it will not be modified. As long as the initial state of all nodes in the cluster is consistent, the final state of the nodes'state machine will also be consistent.Here we implement a state machine called EchoSM, which inherits StateMachine, as follows:
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

In a Paxos packet, multiple state machines can be mounted at the same time, but the same instance data can only be applied to one state machine, and each state machine is identified by a unique SMID.  
Execute() is state transition function of the state machine.Parameter groupidx is used to distinguish different Paxos group.Parameter instanceID is a globally increasing serial number to identify the id of each data item commiting.Parameter paxosValue is the data will be commited.The fourth parameter is an instance of the SMCtx class,it's the context throughout the data commit process.There will be more details introduction in the following.  
WPaxos guarantees that multiple nodes' state machine of the same Paxos group will execute the same paxos value sequence to achieve strong consistency.  

#### Building SMCtx
Following introduced is the SMCtx class introduced before.
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
SMCtx is a context argument which is provided by proposer, transmitted to state machine Execute() function and finally callback to proposer.Variable smId is the target state machine ID to execute.Variable pCtx is the custom context provided by proposer，and can be used by state machine to return execution results.  

The following is the custom context defined in echo sample.
```
public class EchoSMCtx {
    /*the return code of state machine Execute() function*/
	public int executeRet;
	/*the response value transmitted by state machine*/
	private byte[] echoRespValue;
}
```
The above two parameters are both used to transmit the state machine execution results.  

#### Initialize and running WPaxos  
After building the echo state machine, let's init and run WPaxos.  
As follows,the EchoServer class encapsulates initialization, startup and other interfaces of Paxos node.

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
Firstly, we will introduce the following parameters,Parameter myNode represents the information of this node such as ip,port,etc.Parameter nodeList represents all nodes information in the paxos group.Parameter indexType represents the index storage type(File,LevelDB),that can be choosen according to your own scene.Parameter groupCount represents how many Paxos groups to run at the same time.Each group is running completely independent.Function runPaxos() initializes and starts a WPaxos instance.The Options variable contains all arguments and options for running WPaxos instance.  
Then custom state machines need to be mounted after Options vairable has been inited.The GroupSMInfo class saves all state machines of a Paxos instance，in which groupidx identifies the Paxos group instance ID and smlist is the state machine list that has been mounted.  
Finally, calling the runNode() function to start WPaxos.  

#### Proposing Requests  
Here we call the echo() function to propose a value.
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
Firstly,defining the request context EchoSMCtx, and then seting it to the pctx variable of smctx,with seting smID=1,that represents proposing data to be executed by state machine whose SMID is 1;  
Then calling the propose function of paxosNode to initiate a data synchronization request, the first parameter is groupIdx, specifying the Paxos group to be executed, the second parameter is the actual to be transmitted data, and the third parameter is the smCtx we has defined.  
In some complex application scenes, if there are multiple threads proposing request concurrently in the same Paxos group, you can call the batchproposal interface instead, and use asynchronous threads for batch merging, which can greatly improve the performance and throughput.  

#### Running results  
We start a WPaxos cluster with three nodes.Following output comes from the node who proposed the value:
```
echo server start, ip [127.0.0.1] port [30000]
please input : 
1
[SM Execute] ok, smid 1 instanceid 5 value wpaxos_test0
echo response : wpaxos_test0
```

Here we started a echo server listened on 127.0.0.1:30000 and sent paxosValue "wpaxos_test0".Then Execute() funtion successfully printed "[SM Execute] ok...",and we also got the same reponse as propose value from the context variable at the same time.  

Let's see the response from other nodes:
```
echo server start, ip [127.0.0.1] port [30001]
please input : 
[SM Execute] ok, smid 1 instanceid 5 value wpaxos_test0
```
The execution results from the other two nodes are also the same as the propose value.

#### Master Election  
Master a an special role in WPaxos cluster.At any moment, there is only one node of the paxos group that considers itself as master at most.In most distributed systems, the master node is usually elected to provide services or synchronize data to other nodes. WPaxos provides the feature of master automatic election mechanism.  

The following will show you how to use the Master Election into your own service.  
First, we construct a election class ElectionServer.  

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
What's the difference between Echo Sampe and Master election on the implement of runPaxos() is there is no necessary to build your own state machine,but use the inside MasterStateMachine by setting the useMaster attribute of GroupSMInfo to true.  
Then every node can get the master node by call getMasterNode() function, and call isMaster() function to judge whether the current node is master.  

#### Membership changes  
When the Paxos instance is running,you can dynamically add or delete cluster members. Similarly, you needn't to build a separate state machine, but use the inside SystemVSM state machine.  
You can call the add member and delete member functions of paxosNode,as follows:  

```
paxosNode.addMember(groupIdx, node);
paxosNode.removeMember(groupIdx, node);
```

#### Others    
The above is a simple echo example.Some advanced applications, such as the use of data snapshot checkpoint, need to combine with different application scenes, here it will not be introducted in detail.If necessary, please refer to the relevant implementation in the KV storage sample.  

## Document  
[DOCUMENT](DOCUMENT.md)

## Contribution  
[CONTRIBUTING](CONTRIBUTING.md)

## Acknowledgement  
Thanks to Tencent WeChat team for opening up such an excellent C++ PhxPaxos implements;  
Thanks to the [Apache RocketMQ](https://github.com/apache/rocketmq) team for their efficient and flexible design of the file management;   

## License  
WPaxos is licensed under the [Apache License 2.0](./LICENSE).The source code has referenced to the file management part implement of Apache rocketmq storage, whose open license also is the Apache  License 2.0.

## Contact  
<img src="img/wpaxos-wechat.jpg"/>  
Welcome to add the wechat account of wpaxos assistant and join the wpaxos Technology Group for further communication.
