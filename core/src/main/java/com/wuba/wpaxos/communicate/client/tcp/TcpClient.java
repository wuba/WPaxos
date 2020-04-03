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
package com.wuba.wpaxos.communicate.client.tcp;

import static org.jboss.netty.buffer.ChannelBuffers.directBuffer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;

import com.wuba.wpaxos.communicate.client.IClient;
import com.wuba.wpaxos.communicate.config.ClientConfig;
import com.wuba.wpaxos.proto.ProtocolConst;

/**
 * tcp client for send msg
 */
public class TcpClient implements IClient {
	private final Logger logger = LogManager.getLogger(TcpClient.class); 
	private ClientBootstrap bootstrap = null;
	private static ChannelBuffer delimiterBuf = directBuffer(ProtocolConst.DELIMITER.length);
	private ClientConfig config = null;
	
	static {
		delimiterBuf.writeBytes(ProtocolConst.DELIMITER);
	}
	
	public TcpClient(ClientConfig clientConfig) {
		this.config = clientConfig;
		this.init();
	}

	@Override
	public void init() {
		final boolean tcpNoDelay = true;
		bootstrap = new ClientBootstrap();
		bootstrap.setFactory(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), 
				Executors.newCachedThreadPool(), config.getWorkerCountTcp()));
		
		try {
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				@Override
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline cp = Channels.pipeline();
					cp.addLast("framer", new DelimiterBasedFrameDecoder(config.getMaxPakageSizeTcp(), true, delimiterBuf));
					cp.addLast("handler", new TcpClientHander());
					return cp;
				}
			});
			
			bootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
			bootstrap.setOption("child.receiveBufferSize", config.getRecvBufferSizeTcp());
			bootstrap.setOption("child.sendBufferSize", config.getSendBufferSizeTcp());
			bootstrap.setOption("child.writeBufferHighWaterMark", config.getWriteBufferHighWaterMark());
			bootstrap.setOption("child.writeBufferLowWaterMark", config.getWriteBufferLowWaterMark());
			bootstrap.setOption("child.connectTimeoutMillis", config.getConnectTimeout());
		} catch(Exception e) {
			logger.error("TcpClient init error.", e);
		}
	}
	
	/**
	 * 获取指定ip和端口的netty channel
	 * @param server
	 * @param port
	 * @return
	 * @throws Exception
	 */
	@Override
	public Channel getChannel(String server, int port) throws Exception {
		InetSocketAddress socketAddress = null;
		socketAddress = new InetSocketAddress(server, port);
		ChannelFuture future = bootstrap.connect(socketAddress).sync();
		Channel channel = future.getChannel();
		
		return channel;
	}

}
