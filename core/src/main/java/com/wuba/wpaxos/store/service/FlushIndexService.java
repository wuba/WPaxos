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

import com.wuba.wpaxos.store.DefaultLogStorage;
import com.wuba.wpaxos.store.db.FileIndexDB;
import com.wuba.wpaxos.utils.ServiceThread;

/**
 * index file异步刷盘
 */
public class FlushIndexService extends ServiceThread {
	private static final Logger log = LogManager.getLogger(FlushIndexService.class);
	private static final int RetryTimesOver = 3;
	private long lastFlushTimestamp = 0;
	private DefaultLogStorage fileLogStorage;
	private int groupId;
	private FileIndexDB fileIndexDB;

	public FlushIndexService(DefaultLogStorage fileLogStorage, int groupId, FileIndexDB fileIndexDB) {
		super(FlushIndexService.class.getSimpleName() + "-" + groupId);
		this.fileLogStorage = fileLogStorage;
		this.groupId = groupId;
		this.fileIndexDB = fileIndexDB;
	}

	private void doFlush(int retryTimes) {
		/**
		 * 变量含义：如果大于0，则标识这次刷盘必须刷多少个page，如果=0，则有多少刷多少
		 */
		int flushIndexLeastPages = fileLogStorage.getStoreConfig().getFlushIndexDBLeastPages();

		if (retryTimes == RetryTimesOver) {
			flushIndexLeastPages = 0;
		}

		long indexTimestamp = 0;

		// 定时刷盘
		int flushIndexThoroughInterval = fileLogStorage.getStoreConfig().getFlushIndexThoroughInterval();
		long currentTimeMillis = System.currentTimeMillis();
		if (currentTimeMillis >= (this.lastFlushTimestamp + flushIndexThoroughInterval)) {
			this.lastFlushTimestamp = currentTimeMillis;
			flushIndexLeastPages = 0;
			indexTimestamp = fileLogStorage.getStoreCheckpoint().getIndexDBTimestamp(groupId);
		}

		if (fileIndexDB != null) {
			fileIndexDB.flush(flushIndexLeastPages);
		}

		if (0 == flushIndexLeastPages) {
			if (indexTimestamp > 0) {
				fileLogStorage.getStoreCheckpoint().setIndexDBTimestamp(groupId, indexTimestamp);
			}
			fileLogStorage.getStoreCheckpoint().flush();
		}
	}

	@Override
	public void run() {
		log.info(this.getServiceName() + " service started");

		while (!this.isStopped()) {
			try {
				int interval = fileLogStorage.getStoreConfig().getFlushIntervalIndexdb();
				this.waitForRunning(interval);
				this.doFlush(1);
			} catch (Exception e) {
				log.warn(this.getServiceName() + " service has exception. ", e);
			}
		}

		// 正常shutdown时，要保证全部刷盘才退出
		this.doFlush(RetryTimesOver);

		log.info(this.getServiceName() + " service end");
	}

	@Override
	public String getServiceName() {
		return FlushIndexService.class.getSimpleName() + "-" + groupId;
	}

	@Override
	public long getJointime() {
		return 1000 * 60;
	}
}
