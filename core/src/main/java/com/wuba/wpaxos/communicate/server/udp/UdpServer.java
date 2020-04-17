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
package com.wuba.wpaxos.communicate.server.udp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictor;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;

import com.wuba.wpaxos.communicate.config.ServerConfig;
import com.wuba.wpaxos.communicate.server.IServer;

/**
 * udp server for receive msg
 */
public class UdpServer implements IServer {
	private static final Logger logger = LogManager.getLogger(UdpServer.class);
	private Channel channel;
	/** NETTY ServerBootstrap */
	private static  ConnectionlessBootstrap bootstrap;
	public static final ChannelGroup allChannels = new DefaultChannelGroup("Wpaxos.Udp");
	private AtomicBoolean startFlag = new AtomicBoolean(false);
	private ServerConfig config = null;
	
	public UdpServer(ServerConfig config) {
		this.config = config;
	}
	
	/**
	 * Start UDP Server
	 */
	@Override
	public void start() throws Exception {
		logger.info("Udp Server start init...");
		initUdpServer(this.config.getsListenIp(), this.config.getListenPort());
		logger.info("Udp Server start success...");
	}

	/**
	 * INIT UDP Server
	 * @throws Exception 
	 */
	private void initUdpServer(String ip, int port) throws Exception {
		DatagramChannelFactory udpChannelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(), config.getWorkerCountUdp());
		bootstrap = new ConnectionlessBootstrap(udpChannelFactory);
		bootstrap.setOption("sendBufferSize", config.getSendBufferSizeUdp());
		bootstrap.setOption("receiveBufferSize", config.getRecvBufferSizeUdp());//1024*1024*15
		bootstrap.setOption("receiveBufferSizePredictor", new FixedReceiveBufferSizePredictor(config.getRecvBufferSizeUdp()));
		bootstrap.setPipelineFactory(new UdpPipelineFactory(new UdpHandler(), config.getMaxPakageSizeUdp()));
		
		try {
        	SocketAddress socketAddress = new InetSocketAddress(ip, port);
			channel = bootstrap.bind(socketAddress);
			allChannels.add(channel);
            logger.info("UdpServer listening is start! local:"+ ip +" port:" + port);
		} catch (Exception e) {
			logger.error("Init Udp Server error", e);
			System.exit(1);
		}	
	}

	/**
	 * Stop UDP Server
	 */
	@Override
	public void stop() throws Exception {
		logger.info("----------------------------------------------------");
		logger.info("-- Udp Server Closing...");
		startFlag.set(false);
		ChannelGroupFuture future = allChannels.close();
		future.awaitUninterruptibly();
		bootstrap.getFactory().releaseExternalResources();
		logger.info("-- Released external resources");
		logger.info("-- Close Success !");
		logger.info("----------------------------------------------------");
	}
	
}