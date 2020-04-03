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
import org.jboss.netty.channel.ChannelHandler.Sharable;

import com.wuba.wpaxos.communicate.ReceiveMessage;
import com.wuba.wpaxos.communicate.config.ServerConfig;

/**
 * udp server hander for receive msg
 */
@Sharable
public class UdpHandler extends SimpleChannelUpstreamHandler {
	private static final Logger logger = LogManager.getLogger(UdpHandler.class);
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		try {
			ByteBuffer buffer = ((ChannelBuffer)e.getMessage()).toByteBuffer(); 
			byte[] reciveByte = buffer.array();	
			ReceiveMessage receiveMsg = new ReceiveMessage(reciveByte, reciveByte.length);
			receiveMsg.setTimeStamp(System.currentTimeMillis());
			ServerConfig.getInstance().getNetwork().onReceiveMessage(receiveMsg);
		} catch(Exception ex) {
			logger.error("UdpHandler messageReceived error.", ex);
		}
	}
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		super.handleUpstream(ctx, e);
	}
	
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
		UdpServer.allChannels.add(e.getChannel());
		logger.info("udp channel open : {}.", ctx.getChannel().getRemoteAddress());
	}
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		e.getChannel().close();
		logger.error("unexpected exception from downstream remoteAddress(" + e.getChannel().getRemoteAddress().toString() + ")", e.getCause());
	}
	
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e){
		e.getChannel().close();
		UdpServer.allChannels.remove(e.getChannel());
	}
}