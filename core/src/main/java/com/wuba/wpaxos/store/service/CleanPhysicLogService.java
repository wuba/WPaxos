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
import com.wuba.wpaxos.utils.UtilAll;

import java.util.List;

/**
 * 定时删除paxos log文件
 */
public class CleanPhysicLogService extends ServiceThread {
	private static final Logger log = LogManager.getLogger(CleanPhysicLogService.class);
	private DefaultLogStorage logStorage;
    // 手工触发一次最多删除次数
    private static volatile int maxManualDeleteFileTimes = 20;
    private static volatile long lastRedeleteTimestamps = 0;
    // 手工触发删除消息
    // 立刻开始强制删除文件
    private volatile boolean cleanImmediately = false;

    public CleanPhysicLogService (DefaultLogStorage logStorage) {
    	this.logStorage = logStorage;
		maxManualDeleteFileTimes = 0;
		lastRedeleteTimestamps = 0;
    }
   
    /**
     * 手动触发删除过期文件，每次20个
     */
    public void excuteDeleteFilesManualy() {
    	maxManualDeleteFileTimes = 20;
        log.info("excuteDeleteFilesManualy was invoked");
    }

    @Override
    public void run() {
        try {
            this.deleteExpiredFiles();

            this.redeleteHangedFile();
        } catch (Exception e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }
    }

    @Override
    public String getServiceName() {
        return CleanPhysicLogService.class.getSimpleName();
    }

    /**
     * 最前面的文件有可能Hang住，定期检查一下
     */
    private void redeleteHangedFile() {
    	int destroyMapedFileIntervalForcibly = logStorage.getStoreConfig().getDestroyMapedFileIntervalForcibly();
    	int interval = logStorage.getStoreConfig().getRedeleteHangedFileInterval();
        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp - lastRedeleteTimestamps > interval) {
        	lastRedeleteTimestamps = currentTimestamp;
        	
    		List<DefaultDataBase> dblist = this.logStorage.getDbList();
    		for (DefaultDataBase dataBase : dblist) {
    			if (!dataBase.isAvailable()) {
    				continue;
    			}
    			PhysicLog physicLog = dataBase.getValueStore();
    			try {
        			physicLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly);
    			} catch (Exception e) {
					log.error("redeleteHangedFile throws exception", e);
				}
    		}         
        }
    }

    private void deleteExpiredFiles() {
    	try {
            int deleteCount = 0;            
            int deletePhysicFilesInterval =	logStorage.getStoreConfig().getDeletePhysicLogFilesInterval();
            int destroyMapedFileIntervalForcibly = logStorage.getStoreConfig().getDestroyMapedFileIntervalForcibly();

            boolean timeup = this.isTimeToDelete();
            boolean spacefull = this.isSpaceToDelete();
    		long fileReservedTime = logStorage.getStoreConfig().getFileReservedTime();
    		boolean manualDelete = maxManualDeleteFileTimes > 0;
    		
            // 删除物理队列文件
            if (timeup || spacefull || manualDelete) {
            	maxManualDeleteFileTimes = maxManualDeleteFileTimes - 1;

                // 是否立刻强制删除文件
                boolean cleanAtOnce = logStorage.getStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info(
                    "begin to delete before {} hours file.  timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",//
                    fileReservedTime,//
                    timeup,//
                    spacefull,//
                    maxManualDeleteFileTimes,//
                    cleanAtOnce);  

                // 小时转化成毫秒
                fileReservedTime *= 60 * 60 * 1000;

        		List<DefaultDataBase> dblist = this.logStorage.getDbList();
        		for (DefaultDataBase dataBase : dblist) {
        			if (!dataBase.isAvailable()) {
        				continue;
        			}
        			PhysicLog physicLog = dataBase.getValueStore();
        			try {
        				deleteCount = physicLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval, destroyMapedFileIntervalForcibly, cleanAtOnce);
						if (deleteCount > 0) {
							physicLog.correctMinInstanceID();
						}
						else if (spacefull) {
						    log.warn("disk space will be full soon, but delete file failed.");
						}
        			} catch (Exception e) {
    					log.error("deleteExpiredFiles throws exception" ,e);
    				}
        		}  
            }
    	} catch(Exception e) {
    		log.error("delete expired files failed", e);
    	}
    }

    /**
     * 是否可以删除文件，空间是否满足
     */
    private boolean isSpaceToDelete() {
        double ratio = logStorage.getStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
        double DiskSpaceWarningLevelRatio = logStorage.getStoreConfig().getDiskSpaceWarningLevelRatio();
        double DiskSpaceCleanForciblyRatio = logStorage.getStoreConfig().getDiskSpaceCleanForciblyRatio();

        cleanImmediately = false;

        // 检测物理文件磁盘空间
        {
            String storeRootPath = logStorage.getStoreConfig().getStoreRootPath();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storeRootPath);
            if (physicRatio > DiskSpaceWarningLevelRatio) {
                boolean diskok = logStorage.getStoreStatus().getAndMakeDiskFull();
                if (diskok) {
                    log.error("physic disk maybe full soon {}, so mark disk full", physicRatio);
                    System.gc();
                }

                cleanImmediately = true;
            } else if (physicRatio > DiskSpaceCleanForciblyRatio) {
                cleanImmediately = true;
            } else {
                boolean diskok = logStorage.getStoreStatus().getAndMakeDiskOK();
                if (!diskok) {
                    log.info("physic disk space OK {}, so mark disk ok", physicRatio);
                }
            }

            if (physicRatio < 0 || physicRatio > ratio) {
                log.info("physic disk maybe full soon, so reclaim space, {}.", physicRatio);
                return true;
            }
        }

        return false;
    } 

    /**
     * 是否可以删除文件，时间是否满足
     */
    private boolean isTimeToDelete() {
        String when = logStorage.getStoreConfig().getDeleteTime();
        if (UtilAll.isItTimeToDo(when)) {
            log.info("it's time to reclaim disk space, {}.", when);
            return true;
        }

        return false;
    }
}
