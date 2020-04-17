/*
 * Copyright (C) 2005-present, 58.com.  All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wuba.wpaxos.communicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.communicate.client.tcp.TcpClient;
import com.wuba.wpaxos.communicate.client.udp.UDPClient;
import com.wuba.wpaxos.communicate.config.ClientConfig;
import com.wuba.wpaxos.communicate.config.ServerConfig;
import com.wuba.wpaxos.communicate.server.tcp.TcpServer;
import com.wuba.wpaxos.communicate.server.udp.UdpServer;
import com.wuba.wpaxos.proto.ProtocolConst;
import com.wuba.wpaxos.utils.ServiceThread;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 默认网络通信实现
 */
public class DFNetWorker extends NetWork {
	private final Logger logger = LogManager.getLogger(DFNetWorker.class); 
	
	private TcpServer tcpServer = null;
	private UdpServer udpServer = null;
	private List<TcpWriteHandler> tcpWriteHandlerList = new ArrayList<TcpWriteHandler>();
	private List<UdpWriteHandler> udpWriteHandlerList = new ArrayList<UdpWriteHandler>();
	private static volatile DFNetWorker instance = null;
	private static Object lock = new Object();
	
	private DFNetWorker() {}
	
	public static DFNetWorker getInstance() {
		if (instance == null) {
			synchronized(lock) {
				if (instance == null) {
					instance = new DFNetWorker();
				}
			}
		}
		return instance;
	}

	public int init(Options options, int iOThreadCount) {
		ServerConfig serverConfig = ServerConfig.getInstance();
		ClientConfig clientConfig = ClientConfig.getInstance();
		serverConfig.setWorkerCountTcp(iOThreadCount);
		serverConfig.setWorkerCountUdp(iOThreadCount);
		serverConfig.setsListenIp(options.getMyNode().getIp());
		serverConfig.setListenPort(options.getMyNode().getPort());
		serverConfig.setNetwork(this);
		
		if (iOThreadCount <= 1) {
			iOThreadCount = 1;
		}
		
		try {
			this.tcpServer = new TcpServer(serverConfig);
			this.udpServer = new UdpServer(serverConfig);
			
			for (int i = 0; i < iOThreadCount; i++) {
				this.tcpWriteHandlerList.add(new TcpWriteHandler("TcpWriteHandler_" + i, clientConfig, options));
				this.udpWriteHandlerList.add(new UdpWriteHandler("UdpWriteHandler_" + i));
			}
		} catch (Exception e) {
			logger.error("DFNetWorker init error", e);
			return -1;
		}

		return 0;
	}

	@Override
	public void runNetWork() throws Exception {
		this.tcpServer.start();
		this.udpServer.start();

		// wait for other node start.
		Thread.sleep(5000);

		for (TcpWriteHandler tcpHandler : this.tcpWriteHandlerList) {
			tcpHandler.start();
		}

		for (UdpWriteHandler udpHandler : this.udpWriteHandlerList) {
			udpHandler.start();
		}
	}

	@Override
	public void stopNetWork() {
		try {
			this.tcpServer.stop();
			this.udpServer.stop();
			
			for (TcpWriteHandler tcpHandler : this.tcpWriteHandlerList) {
				tcpHandler.shutdown();
			}
			
			for (UdpWriteHandler udpHandler : this.udpWriteHandlerList) {
				udpHandler.shutdown();
			}
		} catch (Exception e) {
			logger.error("DFNetWorker stopNetWork error.", e);
		}	
	}

	@Override
	public int sendMessageTCP(int groupIdx, String ip, int port, byte[] message) {
		try {
			int idx = groupIdx % this.tcpWriteHandlerList.size();
			TcpWriteHandler tcpWriteHandler = this.tcpWriteHandlerList.get(idx);
			SendWindow sendWd = new SendWindow();
			sendWd.setIp(ip);
			sendWd.setPort(port);
			sendWd.setSendBuf(message);
			sendWd.setSendSize(message.length);
			sendWd.setGroupId(groupIdx);
			sendWd.setTimestamp(System.currentTimeMillis());
			
			return tcpWriteHandler.offer(sendWd) ? 0 : -1;
		} catch (Exception e) {
			logger.error("DFNetWorker sendMessageTCP error.", e);
		}
		
		return -1;
	}

	@Override
	public int sendMessageUDP(int groupIdx, String ip, int port, byte[] message) {
		try {
			int idx = groupIdx % this.udpWriteHandlerList.size();
			UdpWriteHandler ucpWriteHandler = this.udpWriteHandlerList.get(idx);
			SendWindow sendWd = new SendWindow();
			sendWd.setIp(ip);
			sendWd.setPort(port);
			sendWd.setSendBuf(message);
			sendWd.setSendSize(message.length);
			sendWd.setGroupId(groupIdx);
			sendWd.setTimestamp(System.currentTimeMillis());
			
			return ucpWriteHandler.offer(sendWd) ? 0 : -1;
		} catch (Exception e) {
			logger.error("DFNetWorker sendMessageUDP error.", e);
		}
		
		return -1;
	}

	@Override
	public void setCheckNode(int group, Set<NodeInfo> nodeInfos){
		int index = group % this.tcpWriteHandlerList.size();
		TcpWriteHandler tcpWriteHandler = this.tcpWriteHandlerList.get(index);
		tcpWriteHandler.setCheckNodeSet(nodeInfos);
	}
}

class TcpWriteHandler extends ServiceThread {
	private final static Logger logger = LogManager.getLogger(TcpWriteHandler.class); 
	private TcpClient tcpClient = null;
	private ClientConfig clientConfig = null;
	private Set<NodeInfo> checkNodeSet = new HashSet<NodeInfo>();
	private ConcurrentHashMap<Long, Channel> tcpClientMap = new ConcurrentHashMap<Long, Channel>(); 
	private LinkedBlockingQueue<SendWindow> sendQueue = new LinkedBlockingQueue<SendWindow>();
	public static AtomicLong memSizeCt = new AtomicLong();
	private static final ScheduledExecutorService scheduledExecutorService = Executors
	.newSingleThreadScheduledExecutor(new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, "TcpWriteHandlerScheduledThread");
		}
	});
	
	public TcpWriteHandler(String name, ClientConfig clientConfig, Options options) {
		this.clientConfig = clientConfig;
		this.tcpClient = new TcpClient(this.clientConfig);
		initChannel(options.getNodeInfoList(), options.getMyNode());
		checkChannelStateSchedule(this);
	}

	public boolean offer(SendWindow sendWd) {
		if (memSizeCt.get() > 0) {
			
		}
		sendQueue.offer(sendWd);
		memSizeCt.addAndGet(sendWd.getSendSize());
		return true;
	}
	
	public void initChannel(List<NodeInfo> nodeList, NodeInfo myNodeinfo) {
		if (nodeList == null) {
			logger.error("No node exist!!!");
			return;
		}
		for (NodeInfo nodeInfo : nodeList) {
			if (nodeInfo.getIpPort().equals(myNodeinfo.getIpPort())) {
				continue;
			}
			String ip = nodeInfo.getIp();
			int port = nodeInfo.getPort();
			long nodeID;
			try {
				nodeID = NodeInfo.parseNodeID(ip, port);
				Channel channel = createTcpChannel(ip, port);
				if (channel != null && channel.isOpen()) {
					tcpClientMap.put(nodeID, channel);
					logger.info("init tcp channel : {}.", nodeInfo.getIpPort());
				}
			} catch (Exception e) {
				logger.error("init channel failed, nodeInfo : {}.", nodeInfo.getIpPort(), e);
			}
			
			this.checkNodeSet.add(nodeInfo);
		}
	}
	
	@Override
	public void run() {
		while (true) {
			final SendWindow sendWd;
			try {
				sendWd = sendQueue.poll(1000, TimeUnit.MILLISECONDS);
				if (sendWd != null) {
					long nodeID = NodeInfo.parseNodeID(sendWd.getIp(), sendWd.getPort());
					Channel channel = this.tcpClientMap.get(nodeID);
					if (channel == null || !channel.isOpen()) {
						continue;
					}
					
					long cost = System.currentTimeMillis() - sendWd.getTimestamp();
					if (cost > 100) {
						logger.info("TRACE before tcp sendmsg execute costs : {}.", cost);
					}
					
					ChannelFuture channelFuture = channel.write(ChannelBuffers.copiedBuffer(sendWd.getSendBuf(), ProtocolConst.DELIMITER));
					channelFuture.addListener(new ChannelFutureListener() {

						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (!future.isSuccess()) {
								logger.error("TcpWriteHandler send msg failed.");
							} else {
								logger.debug("TcpWriteHandler send msg success.");
								long cost = System.currentTimeMillis() - sendWd.getTimestamp();
								if (cost > 200) {
									logger.info("TRACE Tcp sendmsg execute costs : {}.", cost);
								}
							}
						}
					});
				}
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			} catch (Throwable th) {
				logger.error(th.getMessage(), th);
			}
		}
	}
	
	public Channel createTcpChannel(String ip, int port) {
		Channel channel = null;
		try {
			channel = tcpClient.getChannel(ip, port);
		} catch(Exception e) {
			logger.error("create tcp channel failed, ip : {}, port : {}.", ip, port, e);
		}

		return channel;
	}

	public void checkChannelStateSchedule(final TcpWriteHandler tcpWriteHandler) {
		scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				Set<Long> nodeIds = new HashSet<>();
				ConcurrentHashMap<Long, Channel> channelMap = tcpWriteHandler.getTcpClientMap();
				Set<NodeInfo> checkNodeSet = tcpWriteHandler.getCheckNodeSet();
				Iterator<NodeInfo> iter = checkNodeSet.iterator();
				while (iter.hasNext()) {
					NodeInfo nodeInfo = iter.next();
					try {
						long nodeID = NodeInfo.parseNodeID(nodeInfo.getIp(), nodeInfo.getPort());
						nodeIds.add(nodeID);
						Channel channel = channelMap.get(nodeID);
						
						if (channel != null && !channel.isOpen()) {
							channel.close();
							channel = null;
						}
						if (channel == null) {
							try {
								channel = tcpWriteHandler.createTcpChannel(nodeInfo.getIp(), nodeInfo.getPort());
								if (channel != null && !channel.isOpen()) {
									channel.close();
									channel = null;
								} 
								if (channel != null) {
									channelMap.put(nodeID, channel);
									logger.info("checkChannelStateSchedule create new channel {} ", nodeInfo);
								}
							} catch (Exception e) {
								logger.error("tcpChannel create failed, ip {}, port {}, {}.", nodeInfo.getIp(), nodeInfo.getPort(), e);
							}
						}
					} catch(Exception e) {
						logger.error("checkChannelStateSchedule failed, node : " + nodeInfo, e);
					}
				}

				try {
					for (Map.Entry<Long, Channel> entry : channelMap.entrySet()) {
						if (!nodeIds.contains(entry.getKey())) {
							logger.info("checkChannelStateSchedule remove channel nodeid {}",entry.getKey());
							Channel channel = channelMap.get(entry.getKey());
							if (channel != null && !channel.isOpen()) {
								channel.close();
							}
							channel = null;
							channelMap.remove(entry.getKey());
						}
					}
				}catch(Exception e){
					logger.error("checkChannelStateSchedule remove member channel error",e);
				}
			}
			
		}, 1, 5, TimeUnit.SECONDS);
	}
	
	public ConcurrentHashMap<Long, Channel> getTcpClientMap() {
		return tcpClientMap;
	}

	public void setTcpClientMap(ConcurrentHashMap<Long, Channel> tcpClientMap) {
		this.tcpClientMap = tcpClientMap;
	}
	
	public Set<NodeInfo> getCheckNodeSet() {
		return checkNodeSet;
	}

	public void setCheckNodeSet(Set<NodeInfo> checkNodeSet) {
		this.checkNodeSet = checkNodeSet;
	}

	@Override
	public void shutdown() {
		super.stop();
		for (Channel channel : this.tcpClientMap.values()) {
			channel.close();
		}
		this.tcpClientMap.clear();
	}

	@Override
	public String getServiceName() {
		return TcpWriteHandler.class.getName();
	}
}

class UdpWriteHandler extends ServiceThread { 
	private final static Logger logger = LogManager.getLogger(UdpWriteHandler.class); 
	private ConcurrentHashMap<Long, UDPClient> udpClientMap = new ConcurrentHashMap<Long, UDPClient>(); 
	private LinkedBlockingQueue<SendWindow> sendQueue = new LinkedBlockingQueue<SendWindow>();
	
	public UdpWriteHandler(String name) {
	}

	public boolean offer(SendWindow sendWd) {
		sendQueue.offer(sendWd);
		return true;
	}
	
	@Override
	public void run() {
		while (true) {
			SendWindow sendWd;
			try {
				sendWd = sendQueue.poll(1000, TimeUnit.MILLISECONDS);
				if (sendWd != null) {
					UDPClient udpClient = this.getUdpClient(sendWd.getIp(), sendWd.getPort());
					udpClient.send(sendWd.getSendBuf(), ProtocolConst.DELIMITER);
				}
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			} catch (Throwable th) {
				logger.error(th.getMessage(), th);
			}
		}
	}
	
	public UDPClient getUdpClient(String ip, int port) {
		try {
			long nodeID = NodeInfo.parseNodeID(ip, port);
			UDPClient preUdpClient = this.udpClientMap.get(nodeID);
			if (preUdpClient == null) {
				preUdpClient = UDPClient.getInstrance(ip, port, "utf-8");
				this.udpClientMap.put(nodeID,preUdpClient);
			}
			return preUdpClient;
		} catch(Exception e) {
			logger.error("getUdpClient error.", e);
		}
		return null;
	}
	
	@Override
	public void shutdown() {
		super.stop();
		for (UDPClient udpClient : this.udpClientMap.values()) {
			udpClient.close();
		}
		this.udpClientMap.clear();
	}

	@Override
	public String getServiceName() {
		return UdpWriteHandler.class.getName();
	}
}

class SendWindow {
	private byte[] sendBuf;
	private String ip;
	private int port;
	private int sendSize;
	private int groupId;
	
	private long timestamp;
	
	public byte[] getSendBuf() {
		return sendBuf;
	}
	
	public void setSendBuf(byte[] sendBuf) {
		this.sendBuf = sendBuf;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getSendSize() {
		return sendSize;
	}

	public void setSendSize(int sendSize) {
		this.sendSize = sendSize;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getGroupId() {
		return groupId;
	}

	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}
}
