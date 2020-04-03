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

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.store.DefaultDataBase;
import com.wuba.wpaxos.store.DefaultLogStorage;
import com.wuba.wpaxos.store.PhysicLog;
import com.wuba.wpaxos.store.db.IndexDB;
import com.wuba.wpaxos.utils.ServiceThread;

/**
 * 定时删除 paxos log索引文件
 */
public class CleanIndexDBService extends ServiceThread {
	private static final Logger log = LogManager.getLogger(CleanIndexDBService.class);
	private DefaultLogStorage defaultLogStorage;
    
    public CleanIndexDBService(DefaultLogStorage defaultLogStorage) {
    	this.defaultLogStorage = defaultLogStorage;
    }

    private void deleteExpiredFiles() {
        int maxDeleteIndexIdInterval = defaultLogStorage.getStoreConfig().getDeleteIndexIdInterval();
        
		List<DefaultDataBase> dblist = this.defaultLogStorage.getDbList();
		for (DefaultDataBase database : dblist) {
			if (!database.isAvailable()) {
				continue;
			}
			
			PhysicLog physicLog = database.getValueStore();
			IndexDB indexDB = database.getIndexdb();
			
			long minChosenInstanceID = database.getMinChosenInstanceId();
			long instanceID = physicLog.getMinInstanceID() - 1;
			while (instanceID > minChosenInstanceID) {
				long maxDeleteInstanceID = Math.min(instanceID, minChosenInstanceID + maxDeleteIndexIdInterval);
				long deleteInterval = maxDeleteInstanceID - minChosenInstanceID;
				indexDB.deleteExpire(new WriteOptions(false), maxDeleteInstanceID);
				
				minChosenInstanceID = maxDeleteInstanceID;
				WriteOptions writeOptions = new WriteOptions();
				writeOptions.setSync(true);
				int ret = this.defaultLogStorage.setMinChosenInstanceID(writeOptions, database.getMyGroupIdx(), maxDeleteInstanceID);
				if(ret != 0) {
					log.error("logStore setMinChosenInstanceID error, ret={}.", ret);
					return;
				}
				
				if (deleteInterval >= maxDeleteIndexIdInterval) {
	                try {
	                    Thread.sleep(10);
	                }
	                catch (InterruptedException e) { 
	                	log.error(e.getMessage(), e);
	                }
				}				
			}
		}
    }

    @Override
    public void run() {
        try {
            this.deleteExpiredFiles();
        } catch (Exception e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }
    }

    @Override
    public String getServiceName() {
        return CleanIndexDBService.class.getSimpleName();
    }
}
