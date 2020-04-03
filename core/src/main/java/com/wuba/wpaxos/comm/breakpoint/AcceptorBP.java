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
 * Acceptor执行断点跟踪
 */
public class AcceptorBP {
	private static final Logger logger = LogManager.getLogger(AcceptorBP.class);

	public void onPrepare(int groupId, long instanceID) {
		logger.debug("AcceptorBP onPrepare, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onPreparePass(int groupId, long instanceID) {
		logger.debug("AcceptorBP onPreparePass, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onPreparePersistFail(int groupId, long instanceID) {
		logger.debug("AcceptorBP onPreparePersistFail, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onPrepareReject(int groupId, long instanceID) {
		logger.debug("AcceptorBP onPrepareReject, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onAccept(int groupId, long instanceID) {
		logger.debug("AcceptorBP onAccept, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onAcceptPass(int groupId, long instanceID) {
		logger.debug("AcceptorBP onAcceptPass, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onAcceptPersistFail(int groupId, long instanceID) {
		logger.debug("AcceptorBP onAcceptPersistFail, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onAcceptReject(int groupId, long instanceID) {
		logger.info("AcceptorBP onAcceptReject, group : {}, instanceID {}.", groupId, instanceID);
	}

}
