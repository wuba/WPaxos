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

import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.wuba.wpaxos.communicate.ReceiveMessage;
import com.wuba.wpaxos.communicate.config.ServerConfig;

/**
 * Tcp server handler
 */
public class TcpHandler extends SimpleChannelUpstreamHandler {
	private static final Logger logger = LogManager.getLogger(TcpHandler.class);

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		ByteBuffer buffer = ((ChannelBuffer) e.getMessage()).toByteBuffer();
		byte[] reciveByte = buffer.array();	
		ReceiveMessage receiveMsg = new ReceiveMessage(reciveByte, reciveByte.length);
		receiveMsg.setChannel(e.getChannel());
		receiveMsg.setTimeStamp(System.currentTimeMillis());
		ServerConfig.getInstance().getNetwork().onReceiveMessage(receiveMsg);
	}
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		if (e instanceof ChannelStateEvent) {
			logger.debug(e.toString());
		}
		super.handleUpstream(ctx, e);
	}
	
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		TcpServer.allChannels.add(e.getChannel());
		if (e.getChannel() != null && e.getChannel().getRemoteAddress() != null) {
			logger.info("new channel open: {}.", e.getChannel().getId());
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		logger.error("unexpected exception from downstream remoteAddress(" + e.getChannel().getRemoteAddress().toString() + ")", e.getCause());
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
		logger.info("channel is closed: {}.", e.getChannel().getRemoteAddress().toString());
		TcpServer.allChannels.remove(e.getChannel());
	}
}
