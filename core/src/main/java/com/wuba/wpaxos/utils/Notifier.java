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
package com.wuba.wpaxos.utils;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.helper.SerialLock;

public class Notifier {
	private final Logger logger = LogManager.getLogger(Notifier.class); 
	private long id;
	private int ret = -1;
	
	private int commitRet = -1;
	private boolean isCommitEnd = false;
	private SerialLock serialLock = new SerialLock();
	
	public void init() {
		this.id = Thread.currentThread().getId();
		serialLock.lock();
		this.commitRet = -1;
		this.isCommitEnd = false;
		serialLock.unLock();
	}
	
    public int waitNotify() {
		this.serialLock.lock();
		try {
			while (!isCommitEnd) {
				this.serialLock.waitTime(1000);
			}
		} catch(Exception e) {
			logger.error("", e);
		} finally {
			this.serialLock.unLock();
		}
		return this.commitRet;
    }
    
    public void sendNotify(int ret) throws IOException {
		serialLock.lock();
		this.commitRet = ret;
		this.isCommitEnd = true;
		serialLock.interupt();
		serialLock.unLock();
    }
    
    public int getRet() {
    	return ret;
    }
    
    public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

}
