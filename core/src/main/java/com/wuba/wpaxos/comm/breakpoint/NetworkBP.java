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
package com.wuba.wpaxos.comm.breakpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 网络模块断点日志跟踪
 */
public class NetworkBP {

	private static final Logger logger = LogManager.getLogger(NetworkBP.class);

	public void tcpEpollLoop(int groupId) {
		logger.debug("NetworkBP tcpEpollLoop, group : {}.", groupId);
	}

	public void tcpOnError(int groupId) {
		logger.debug("NetworkBP tcpOnError, group : {}.", groupId);
	}

	public void tcpAcceptFd(int groupId) {
		logger.debug("NetworkBP tcpAcceptFd, group : {}.", groupId);
	}

	public void tcpQueueFull(int groupId) {
		logger.debug("NetworkBP tcpQueueFull, group : {}.", groupId);
	}

	public void tcpReadOneMessageOk(int len, int groupId) {
		logger.debug("NetworkBP tcpReadOneMessageOk, group : {}.", groupId);
	}

	public void tcpOnReadMessageLenError(int groupId) {
		logger.debug("NetworkBP tcpOnReadMessageLenError, group : {}.", groupId);
	}

	public void tcpReconnect(int groupId) {
		logger.debug("NetworkBP tcpReconnect, group : {}.", groupId);
	}

	public void tcpOutQueue(int delayMs, int groupId) {
		logger.debug("NetworkBP tcpOutQueue delayMs= {}.", delayMs);
	}

	public void sendRejectByTooLargeSize(int groupId) {
		logger.debug("NetworkBP sendRejectByTooLargeSize, group {}.", groupId);
	}

	public void send(byte[] message, int groupId) {
		logger.debug("NetworkBP send message length = {}.", (message == null ? 0 : message.length));
	}

	public void sendTcp(byte[] message, int groupId) {
		logger.debug("NetworkBP sendTcp message length = {}.", (message == null ? 0 : message.length));
	}

	public void sendUdp(byte[] message, int groupId) {
		logger.debug("NetworkBP sendUdp message length = {}.", (message == null ? 0 : message.length));
	}

	public void sendMessageNodeIDNotFound(int groupId) {
		logger.debug("NetworkBP sendMessageNodeIDNotFound, group : {}.", groupId);
	}

	public void udpReceive(int recvLen, int groupId) {
		logger.debug("NetworkBP udpReceive recvLen= {}.", recvLen);
	}

	public void udpRealSend(String message, int groupId) {
		logger.debug("NetworkBP udpRealSend message lenth = {}.", (message == null ? 0 : message.length()));
	}

	public void udpQueueFull(int groupId) {
		logger.debug("NetworkBP udpQueueFull, group : {}.", groupId);
	}

}
