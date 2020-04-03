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
package com.wuba.wpaxos.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.storemachine.SMFac;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.OtherUtils;
import com.wuba.wpaxos.utils.Time;

/**
 * 历史数据清理任务
 */
public class Cleaner implements Runnable {
	private static final Logger logger = LogManager.getLogger(Cleaner.class);

	private static final int CAN_DELETE_DELTA = 1000000;
	private static final int DELETE_SAVE_INTERVAL = 100;

	private Config config;
	private SMFac smFac;
	private LogStorage logStorage;
	private CheckpointMgr checkpointMgr;
	private long lastSave;
	private boolean canRun;
	private boolean isPaused;
	private boolean isEnd;
	private boolean isStart;
	private long holdCount;

	public Cleaner(Config config, SMFac smFac, LogStorage logStorage, CheckpointMgr checkpointMgr) {
		this.config = config;
		this.smFac = smFac;
		this.logStorage = logStorage;
		this.checkpointMgr = checkpointMgr;
		this.lastSave = 0;
		this.canRun = false;
		this.isPaused = true;
		this.isEnd = false;
		this.isStart = false;
		this.holdCount = CAN_DELETE_DELTA;
	}

	public void stop() throws Exception {
		this.isEnd = true;
		if (this.isStart) {
			join();
		}
	}

	private void join() throws Exception {
		Thread.currentThread().join();
	}

	@Override
	public void run() {
		this.isStart = true;
		toContinue();

		//control delete speed to avoid affecting the io too much.
		int deleteQps = InsideOptions.getInstance().getCleanerDeleteQps();
		int sleepMs = deleteQps > 1000 ? 1 : 1000 / deleteQps;
		int deleteInterval = deleteQps > 1000 ? deleteQps / 1000 + 1 : 1;

		while (true) {
			try {
				if (this.isEnd) {
					logger.info("Checkpoint.Cleaner [END]");
					return;
				}
				
				if (!this.logStorage.isAvailable(this.config.getMyGroupIdx())) {
					Time.sleep(1000);
					continue;
				}

				if (!this.canRun) {
					this.isPaused = true;
					Time.sleep(1000);
					continue;
				}

				long instanceId = this.checkpointMgr.getMinChosenInstanceID();
				long cpInstanceId = this.smFac.getCheckpointInstanceID(this.config.getMyGroupIdx()) + 1;
				long maxChosenInstanceId = this.checkpointMgr.getMaxChosenInstanceID();

				int deleteCount = 0;
				//当MinChosenInstanceID与当前CheckpointInstanceID和maxChosenInstanceId的差距都大于holdCount时，逐个删除
				while((instanceId + this.holdCount < cpInstanceId) &&
						(instanceId + this.holdCount < maxChosenInstanceId)) {
					long maxDeleteInstance = Math.min(cpInstanceId, maxChosenInstanceId) - this.holdCount;
					int maxDeleteCount = (int) (maxDeleteInstance - instanceId);
					maxDeleteCount = Math.min(maxDeleteCount, deleteInterval);
					
					boolean deleteRet = deleteExpire(instanceId + maxDeleteCount);
					if(deleteRet) {
						instanceId += maxDeleteCount;
						deleteCount += maxDeleteCount;
						if (deleteCount >= deleteInterval) {
							deleteCount = 0;
							Time.sleep(sleepMs);
						}
					} else {
						logger.warn("delete system fail, instanceid {}.", instanceId);
						break;
					}
				}

				if (cpInstanceId == 0) {
					logger.debug("sleep a while, max deleted instanceid {} checkpoint instanceid 0(no checkpoint)"
							+ " now instanceid {}.", instanceId, this.checkpointMgr.getMaxChosenInstanceID());
				} else {
					logger.debug("sleep a while, max deleted instanceid {} checkpoint instanceid {}"
							+ " now instanceid {}.", instanceId, cpInstanceId, this.checkpointMgr.getMaxChosenInstanceID());
				}

				Time.sleep(OtherUtils.fastRand() % 500 + 500);
			} catch (Exception e) {
				logger.error("run throws exception", e);
			}
		}
	}

	public void pause() {
		this.canRun = false;
	}

	public void toContinue() {
		this.isPaused = false;
		this.canRun = true;
	}

	public boolean isPaused() {
		return this.isPaused;
	}

	public void setHoldPaxosLogCount(long holdCount) {
		if (holdCount < 300) {
			this.holdCount = 300;
		} else {
			this.holdCount = holdCount;
		}
	}

	/**
	 * 从min chosen instance id开始向最大遍历，直到数据不为空为止
	 * @param oldMinChosenInstanceID
	 * @return
	 */
	public int fixMinChosenInstanceID(long oldMinChosenInstanceID) {
		JavaOriTypeWrapper<Long> maxInstanceIdWrap = new JavaOriTypeWrapper<Long>();
		int ret = this.logStorage.getMaxInstanceID(this.config.getMyGroupIdx(), maxInstanceIdWrap);
		if (ret == 1) {
			return 0;
		} else if (ret == -2) {
			return ret;
		}
		long maxInstanceId = maxInstanceIdWrap.getValue();
		long fixMinChosenInstanceId = oldMinChosenInstanceID;

		ret = 0;
		for (long instanceId = oldMinChosenInstanceID; instanceId < maxInstanceId; instanceId++) {
			JavaOriTypeWrapper<byte[]> valueWrap = new JavaOriTypeWrapper<byte[]>();
			ret = this.logStorage.get(this.config.getMyGroupIdx(), instanceId, valueWrap);
			if (ret != 0 && ret != 1) {
				return -1;
			} else if (ret == 1) {
				logger.info("fixMinChosenInstanceID instanceid {} not found data, delete it. groupid= {}",instanceId, this.config.getMyGroupIdx());
				this.logStorage.deleteOneIndex(this.config.getMyGroupIdx(), instanceId);
				fixMinChosenInstanceId = instanceId + 1;
			} else {
				break;
			}
		}

		if (fixMinChosenInstanceId > oldMinChosenInstanceID) {
			logger.info("fixMinChosenInstanceID set new min chosen instance id {}, old is {}, groupid= {} ", fixMinChosenInstanceId, oldMinChosenInstanceID, this.config.getMyGroupIdx());
			ret = this.checkpointMgr.setMinChosenInstanceID(fixMinChosenInstanceId);
			if (ret != 0) {
				return ret;
			}
		}

		return 0;
	}

	public boolean deleteExpire(long instanceId) {
		WriteOptions writeOptions = new WriteOptions();
		writeOptions.setSync(false);

		int ret = this.logStorage.delExpire(writeOptions, this.config.getMyGroupIdx(), instanceId);
		if (ret != 0) {
			return false;
		}

		this.checkpointMgr.setMinChosenInstanceIDCache(instanceId+1);

		if(instanceId > this.lastSave + DELETE_SAVE_INTERVAL) {
			return persistMinChosenInstanceID(instanceId);
		}

		return true;
	}

	public boolean persistMinChosenInstanceID(long instanceId) {
		int ret;
		ret = this.checkpointMgr.setMinChosenInstanceID(instanceId + 1);
		if (ret != 0) {
			logger.error("SetMinChosenInstanceID fail, now delete instanceid {}.", instanceId);
			return false;
		}

		this.lastSave = instanceId;
		logger.info("delete instance done, now minchosen instanceid {}.", instanceId);

		return true;
	}

}

















