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
 * IOLoop断点日志跟踪
 */
public class IOLoopBP {

	private static final Logger logger = LogManager.getLogger(IOLoopBP.class);

	public void oneLoop(int groupId) {
		logger.debug("IOLoopBP oneLoop, group : {}.", groupId);
	}

	public void enqueueMsg(int groupId) {
		logger.debug("IOLoopBP enqueueMsg, group : {}.", groupId);
	}

	public void enqueueMsgRejectByFullQueue(int groupId) {
		logger.debug("IOLoopBP enqueueMsgRejectByFullQueue, group : {}.", groupId);
	}

	public void enqueueRetryMsg(int groupId) {
		logger.debug("IOLoopBP enqueueRetryMsg, group : {}.", groupId);
	}

	public void enqueueRetryMsgRejectByFullQueue(int groupId) {
		logger.debug("IOLoopBP enqueueRetryMsgRejectByFullQueue, group : {}.", groupId);
	}

	public void outQueueMsg(int groupId) {
		logger.debug("IOLoopBP outQueueMsg, group : {}.", groupId);
	}

	public void dealWithRetryMsg(int groupId) {
		logger.debug("IOLoopBP dealWithRetryMsg, group : {}.", groupId);
	}

}
