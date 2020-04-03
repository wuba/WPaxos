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
 * write index from writebuffer to filechannel
 */
public class CommitIndexService extends ServiceThread {
	private static final Logger log = LogManager.getLogger(CommitIndexService.class);
	private long lastCommitTimestamp = 0L;
	private static final int RetryTimesOver = 10;
	private int groupId;
	private FileIndexDB fileIndexDB;
	private DefaultLogStorage fileLogStorage;

	public CommitIndexService(DefaultLogStorage fileLogStorage, int groupId, FileIndexDB fileIndexDB) {
		super(CommitIndexService.class.getSimpleName() + "-" + groupId);
		this.fileLogStorage = fileLogStorage;
		this.groupId = groupId;
		this.fileIndexDB = fileIndexDB;
	}

	@Override
	public void run() {
		log.info(this.getServiceName() + " service started");

		while (!this.isStopped()) {
			int interval = this.fileLogStorage.getStoreConfig().getCommitIntervalIndexdb();

			int commitDataLeastPages = this.fileLogStorage.getStoreConfig().getCommitIndexDBLeastPages();

			int commitDataThoroughInterval = this.fileLogStorage.getStoreConfig().getCommitIndexToroughInterval();

			long begin = System.currentTimeMillis();
			if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
				this.lastCommitTimestamp = begin;
				commitDataLeastPages = 0;
			}

			try {
				if (fileIndexDB != null) {
					boolean result = fileIndexDB.commit(commitDataLeastPages);
					long end = System.currentTimeMillis();
					if (!result) {
						this.lastCommitTimestamp = end;
						//now wake up flush thread.
						this.fileIndexDB.getFlushIndexService().wakeup();
					}

					if (end - begin > 2000) {
						log.info("Commit indexdb to file costs {} ms", end - begin);
					}
					this.waitForRunning(interval);
				}
			} catch (Throwable e) {
				log.warn(this.getServiceName() + " service has exception. ", e);
			}
		}

		if (fileIndexDB != null) {
			boolean result = false;
			for (int j = 0; j < RetryTimesOver && !result; j++) {
				result = fileIndexDB.commit(0);
				log.info(this.getServiceName() + " service shutdown, group : " + groupId + " retry " + (j + 1) + " times " + (result ? "OK" : "Not OK"));
			}
		}

		log.info(this.getServiceName() + " service end");
	}

	@Override
	public String getServiceName() {
		return CommitIndexService.class.getSimpleName() + "-" + groupId;
	}
}
