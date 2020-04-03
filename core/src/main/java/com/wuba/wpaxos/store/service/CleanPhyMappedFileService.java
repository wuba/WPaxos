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
 * 一定会时间内不再访问的paxoslog文件，取消内存映射
 */
public class CleanPhyMappedFileService extends ServiceThread{
	private static final Logger log = LogManager.getLogger(CleanPhyMappedFileService.class);
	private DefaultLogStorage fileLogStorage;
	
	public CleanPhyMappedFileService(DefaultLogStorage fileLogStorage) {
    	this.fileLogStorage = fileLogStorage;
    }
	
	public void cleanUnMappedFiles() {
		List<DefaultDataBase> dblist = this.fileLogStorage.getDbList();
		for (DefaultDataBase dataBase : dblist) {
			PhysicLog physicLog = dataBase.getValueStore();
			if (physicLog != null) {
				physicLog.getMapedFileQueue().cleanUnMappedFile();
			}
		}
	}
	
	@Override
	public void run() {
		try {
            this.cleanUnMappedFiles();
        } catch (Exception e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }
	}

	@Override
	public String getServiceName() {
		return CleanPhyMappedFileService.class.getSimpleName();
	}
}
