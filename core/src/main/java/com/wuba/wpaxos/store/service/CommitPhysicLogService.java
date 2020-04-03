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
package com.wuba.wpaxos.store.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.store.*;
import com.wuba.wpaxos.utils.ServiceThread;

import java.util.List;

/**
 * write paxoslog from writebuffer to filechannel
 */
public class CommitPhysicLogService extends ServiceThread {
	private static final Logger log = LogManager.getLogger(CommitPhysicLogService.class);
	private long lastCommitTimestamp = 0;
	private static final int RetryTimesOver = 10;
	private DefaultLogStorage logStorage;
	
	public CommitPhysicLogService(DefaultLogStorage logStorage) {
		this.logStorage = logStorage;
	}

	@Override
	public void run() {
		log.info(this.getServiceName() + " service started");
		
		while (!this.isStopped()) {
            int interval = this.logStorage.getStoreConfig().getCommitIntervalPhysicLog();

            int commitDataLeastPages = this.logStorage.getStoreConfig().getCommitPhysicLogLeastPages();

            int commitDataThoroughInterval = this.logStorage.getStoreConfig().getCommitPhysicLogThoroughInterval();

            long begin = System.currentTimeMillis();
            if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                this.lastCommitTimestamp = begin;
                commitDataLeastPages = 0;
            }

			try {
				List<DefaultDataBase> dblist = this.logStorage.getDbList();
				for (DefaultDataBase  dataBase: dblist) {
					PhysicLog physicLog = dataBase.getValueStore();
					if (physicLog != null) {
						boolean result = physicLog.getMapedFileQueue().commit(commitDataLeastPages);
						long end = System.currentTimeMillis();
						if (!result) {
							this.lastCommitTimestamp = end;
							this.logStorage.getFlushRealTimeService().wakeup();
						}
						
	                    if (end - begin > 2000) {
	                        log.info("Commit data to file costs {} ms", end - begin);
	                    }
	                    this.waitForRunning(interval);
					}
				}					
			} catch (Throwable e) {
				log.warn(this.getServiceName() + " service has exception. ", e);
			}
		}
		
		List<DefaultDataBase> dblist = this.logStorage.getDbList();
		for (DefaultDataBase dataBase : dblist) {
			PhysicLog physicLog = dataBase.getValueStore();
			if (physicLog != null) {
				// Normal shutdown, to ensure that all the flush before exit
				boolean result = false;
				for (int i = 0; i < RetryTimesOver && !result; i++) {
					result = physicLog.getMapedFileQueue().commit(0);
					log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
				}
			}
		}

		log.info(this.getServiceName() + " service end");
	}

	@Override
	public String getServiceName() {
		return CommitPhysicLogService.class.getSimpleName();
	}
}
