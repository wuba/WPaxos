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

import com.wuba.wpaxos.store.DefaultDataBase;
import com.wuba.wpaxos.store.DefaultLogStorage;
import com.wuba.wpaxos.store.PhysicLog;
import com.wuba.wpaxos.utils.ServiceThread;

import java.util.List;

/**
 * FlushPhysicLogService(异步刷盘)
 */
public class FlushPhysicLogService extends ServiceThread {
	private static final Logger log = LogManager.getLogger(FlushPhysicLogService.class);
	private static final int RetryTimesOver = 3;
	private long lastFlushTimestamp = 0;
	private long printTimes = 0;
	private DefaultLogStorage logStorage;
	
	public FlushPhysicLogService(DefaultLogStorage logStorage) {
		this.logStorage = logStorage;
	}

	@Override
	public void run() {
		log.info(this.getServiceName() + " service started");

		while (!this.isStopped()) {
			boolean flushPhysicLogTimed = this.logStorage.getStoreConfig().isFlushPhysicTimed();

			int interval = this.logStorage.getStoreConfig().getFlushIntervalPhysicLog();
			int flushPhysicLogLeastPages = this.logStorage.getStoreConfig().getFlushPhysicLogLeastPages();

			int flushPhysicLogThoroughInterval = this.logStorage.getStoreConfig().getFlushPhysicLogThoroughInterval();

			boolean printFlushProgress = false;

			// Print flush progress
			long currentTimeMillis = System.currentTimeMillis();
			if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicLogThoroughInterval)) {
				this.lastFlushTimestamp = currentTimeMillis;
				flushPhysicLogLeastPages = 0;
				printFlushProgress = ((printTimes++ % 10) == 0);
			}

			try {
				if (flushPhysicLogTimed) {
					Thread.sleep(interval);
				} else {
					this.waitForRunning(interval);
				}

				if (printFlushProgress) {
					this.printFlushProgress();
				}

				List<DefaultDataBase> dblist = this.logStorage.getDbList();
				for (DefaultDataBase dataBase : dblist) {
					PhysicLog physicLog = dataBase.getValueStore();
					if (physicLog != null) {
						physicLog.getMapedFileQueue().flush(flushPhysicLogLeastPages);
						long storeTimestamp = physicLog.getMapedFileQueue().getStoreTimestamp();
						if (storeTimestamp > 0) {
							this.logStorage.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
						}
					}
				}			
			} catch (Exception e) {
				log.warn(this.getServiceName() + " service has exception. ", e);
				this.printFlushProgress();
			}
		}

		List<DefaultDataBase> dblist = this.logStorage.getDbList();
		for (DefaultDataBase dataBase : dblist) {
			PhysicLog physicLog = dataBase.getValueStore();
			if (physicLog != null) {
				// Normal shutdown, to ensure that all the flush before exit
				boolean result = false;
				for (int i = 0; i < RetryTimesOver && !result; i++) {
					result = physicLog.getMapedFileQueue().flush(0);
					log.info(this.getServiceName() + " service shutdown, group : " + physicLog.getGroupIdx() +  " retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
				}
			}
		}			

		this.printFlushProgress();
		log.info(this.getServiceName() + " service end");
	}

	@Override
	public String getServiceName() {
		return FlushPhysicLogService.class.getSimpleName();
	}

	private void printFlushProgress() {
		try {
			List<DefaultDataBase> dblist = this.logStorage.getDbList();
			for (DefaultDataBase dataBase : dblist) {
				PhysicLog physicLog = dataBase.getValueStore();
				if (physicLog != null) {
					if (log.isDebugEnabled()) {
						log.debug("physiclog how much disk fall behind memory, {}.", physicLog.getMapedFileQueue().howMuchFallBehind());
					}
				}
			}
		} catch(Exception e) {
			log.error("physiclog printFlushProgress error.", e);
		}
	}

	@Override
	public long getJointime() {
		return 1000 * 60 * 5;
	}
}
