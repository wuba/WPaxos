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
 * checkpoint执行断点跟踪
 */
public class CheckpointBP {
	
	private static final Logger logger = LogManager.getLogger(CheckpointBP.class);

	public void needAskforCheckpoint() {
		logger.debug("CheckpointBP needAskforCheckpoint");
	}

	public void sendCheckpointOneBlock() {
		logger.debug("CheckpointBP sendCheckpointOneBlock");
	}

	public void onSendCheckpointOneBlock() {
		logger.debug("CheckpointBP onSendCheckpointOneBlock");
	}

	public void sendCheckpointBegin() {
		logger.debug("CheckpointBP sendCheckpointBegin");
	}

	public void sendCheckpointEnd() {
		logger.debug("CheckpointBP sendCheckpointEnd");
	}

	public void receiveCheckpointDone() {
		logger.debug("CheckpointBP receiveCheckpointDone");
	}

	public void receiveCheckpointAndLoadFail() {
		logger.debug("CheckpointBP receiveCheckpointAndLoadFail");
	}

	public void receiveCheckpointAndLoadSucc() {
		logger.debug("CheckpointBP receiveCheckpointAndLoadSucc");
	}
}
