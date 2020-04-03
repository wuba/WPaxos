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
package com.wuba.wpaxos.communicate.server.tcp;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.wuba.wpaxos.communicate.config.ServerConfig;
import com.wuba.wpaxos.communicate.server.IServer;

/**
 * tcp server for receive msg
 */
public class TcpServer implements IServer {
	private static final Logger logger = LogManager.getLogger(TcpServer.class);
	private static final ServerBootstrap bootstrap = new ServerBootstrap();
	public static final ChannelGroup allChannels = new DefaultChannelGroup("Wpaxos");
	private ServerConfig config = null;
	
	public TcpServer(ServerConfig config) {
		this.config = config;
	}
	
	@Override
	public void start() throws Exception {
		final boolean tcpNoDelay = true;
		bootstrap.setFactory(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), 
				Executors.newCachedThreadPool(), config.getWorkerCountTcp()));
		bootstrap.setPipelineFactory(new TcpPipelineFactory(new TcpHandler(), config.getMaxPakageSizeTcp()));
		bootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
		bootstrap.setOption("child.receiveBufferSize", config.getRecvBufferSizeTcp());
		bootstrap.setOption("child.sendBufferSize", config.getSendBufferSizeTcp());
		bootstrap.setOption("child.writeBufferHighWaterMark", config.getWriteBufferHighWaterMark());
		bootstrap.setOption("child.writeBufferLowWaterMark", config.getWriteBufferLowWaterMark());

		try {
			InetSocketAddress socketAddress = null;
			socketAddress = new InetSocketAddress(this.config.getsListenIp(), this.config.getListenPort());
			Channel channel = bootstrap.bind(socketAddress);
			allChannels.add(channel);
			logger.info("Tcp server start : {}.", this.config.getsListenIp() + "_" + this.config.getListenPort());
		} catch (Exception e) {
			logger.error("init socket server error", e);
			System.exit(1);
		}
	}
	
	@Override
	public void stop() throws Exception {
		try {
			logger.info("----------------------------------------------------");
			logger.info("-- socket server closing...");
			logger.info("-- channels count : " + allChannels.size());
			ChannelGroupFuture future = allChannels.close();
			logger.info("-- closing all channels...");
			future.awaitUninterruptibly();
			logger.info("-- closed all channels...");
			bootstrap.getFactory().releaseExternalResources();
			logger.info("-- released external resources");
			logger.info("-- close success !");
			logger.info("----------------------------------------------------");
		} catch(Exception ex) {
			logger.error("stop tcpserver failed.", ex);
			throw ex;
		}
	}
}

