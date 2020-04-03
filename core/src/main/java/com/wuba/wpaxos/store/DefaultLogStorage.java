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
package com.wuba.wpaxos.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.GroupSMInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.IndexType;
import com.wuba.wpaxos.comm.enums.PaxosLogCleanType;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.store.pagecache.MapedFile;
import com.wuba.wpaxos.store.service.AllocateMapedFileService;
import com.wuba.wpaxos.store.service.CleanIndexDBService;
import com.wuba.wpaxos.store.service.CleanPhyMappedFileService;
import com.wuba.wpaxos.store.service.CleanPhysicLogService;
import com.wuba.wpaxos.store.service.CommitPhysicLogService;
import com.wuba.wpaxos.store.service.FlushPhysicLogService;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.ThreadFactoryImpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * default log storage realize
 */
public class DefaultLogStorage implements LogStorage {
	private static final Logger logger = LogManager.getLogger(DefaultLogStorage.class);
	private StoreConfig storeConfig;
	private Options options;
	private TransientStorePool transientStorePool;
	private AllocateMapedFileService allocateMapedFileService;
	// 存储检查点
	private StoreCheckpoint storeCheckpoint;
	private FlushPhysicLogService flushPhysicLogService;
	private CommitPhysicLogService commitRealTimeService;
	// 取消mapedfile内存映射服务
	private CleanPhyMappedFileService cleanPhyMappedFileService;	
	private CleanPhysicLogService cleanPhysicLogService;
	private CleanIndexDBService cleanIndexDBService;
	private volatile boolean isInit = false;
	private volatile boolean shutdown = true;
	// 运行过程标志位
	private final StoreStatus storeStatus = new StoreStatus();
	private List<DefaultDataBase> dbList = new ArrayList<>();
	private static volatile DefaultLogStorage defaultLogStorage = null;
	private ScheduledExecutorService scheduledExecutorService;

	private DefaultLogStorage() {
	}

	public static DefaultLogStorage getInstance() {
		if (defaultLogStorage == null) {
			synchronized (DefaultLogStorage.class) {
				if (defaultLogStorage == null) {
					defaultLogStorage = new DefaultLogStorage();
				}
			}
		}
		return defaultLogStorage;
	}

	@Override
	public boolean init(Options options) throws Exception {
		// 只初始化一次
		if (isInit) {
			return true;
		}
		synchronized (DefaultLogStorage.class) {
			if (!isInit) {
				isInit = true;
			} else {
				return true;
			}
		}
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DefaultLogStorageScheduledThread"));
		this.options = options;
		this.storeConfig = initStoreConfig(options.getLogStoragePath(), options.getLogStorageConfPath());

		int groupCount = options.getGroupCount();
		if (this.storeConfig.isTransientStorePoolEnable()) {
			this.transientStorePool = new TransientStorePool(storeConfig, groupCount);
			this.transientStorePool.init();
		}
		this.allocateMapedFileService = new AllocateMapedFileService(transientStorePool, storeConfig);
		this.allocateMapedFileService.start();

		String dbPath = options.getLogStoragePath();
		File file = new File(dbPath);
		if (!file.exists()) {
			file.mkdirs();
		}

		if (groupCount < 1 || groupCount > 100000) {
			throw new Exception("Groupcount [" + groupCount + "] wrong");
		}

		if (!dbPath.endsWith(File.separator)) {
			dbPath += File.separator;
		}
		this.storeCheckpoint = new StoreCheckpoint(storeConfig.getStoreCheckpoint(),groupCount);
		boolean result = true;
		for (int groupId = 0; groupId < groupCount; groupId++) {
			String groupDbPath = dbPath + "g" + groupId;
			DefaultDataBase db = new DefaultDataBase(this, this.storeConfig);
			result = result && db.init(groupDbPath, groupId);
			this.dbList.add(db);
		}

		if (result) {
			this.flushPhysicLogService = new FlushPhysicLogService(this);
			this.commitRealTimeService = new CommitPhysicLogService(this);
			this.cleanPhyMappedFileService = new CleanPhyMappedFileService(this);
			if (options.getPaxosLogCleanType().getType() == PaxosLogCleanType.cleanByTime.getType()) {
				this.cleanPhysicLogService = new CleanPhysicLogService(this);
				this.cleanIndexDBService = new CleanIndexDBService(this);
			}
		}

		if (!result) {
			this.allocateMapedFileService.shutdown();
		}
		
		return result;
	}

	@Override
	public boolean isAvailable(int groupId) {
		if (groupId >= this.dbList.size()) {
			return false;
		}
		
		return this.dbList.get(groupId).isAvailable();
	}

	private StoreConfig initStoreConfig(String storeRootPath, String storeConfPath) {
		return new StoreConfig(storeRootPath, storeConfPath);
	}

	@Override
	public void start() {
		for (DefaultDataBase dataBase : dbList) {
			dataBase.start();
		}
		this.flushPhysicLogService.start();
		if (this.storeConfig.isTransientStorePoolEnable()) {
			this.commitRealTimeService.start();
		}

		try {
			this.createTempFile();
		} catch (Exception e) {
			logger.error("create tmp file error.", e);
		}

		this.addScheduleWork();

		this.shutdown = false;
	}

	@Override
	public void shutdown() {
		if (!this.shutdown) {
			this.shutdown = true;

			if (this.storeConfig.isTransientStorePoolEnable()) {
				this.commitRealTimeService.shutdown();
				this.transientStorePool.destroy();
			}
			for (DefaultDataBase dataBase : dbList) {
				dataBase.shutDown();
			}
			this.flushPhysicLogService.shutdown();
			this.allocateMapedFileService.shutdown();
			this.storeCheckpoint.shutdown();
			this.scheduledExecutorService.shutdown();
		}

		for (DefaultDataBase dataBase : dbList) {
			dataBase.shutDown();
		}
		this.deleteFile(this.storeConfig.getAbortFile());
	}

	private void addScheduleWork() {
		if (options.getIndexType().getType() == IndexType.PHYSIC_FILE.getType()) {
			this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

				@Override
				public void run() {
					for (DefaultDataBase dataBase : dbList) {
						dataBase.clearIndexCache();
					}
				}
			}, 1, 1, TimeUnit.MINUTES);
		}

		// 定时删除过期文件
		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				if (options.getPaxosLogCleanType().getType() == PaxosLogCleanType.cleanByTime.getType()) {
					DefaultLogStorage.this.cleanFilesPeriodically();
				}
			}
		}, 1000 * 60, this.storeConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);
		
		// 定时clean不需要内存映射的mapedfile
		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				DefaultLogStorage.this.unmapFilesPeriodically();
			}
		}, 1000 * 60, this.getStoreConfig().getCheckUnmapfileInterval(), TimeUnit.MILLISECONDS);

		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				DefaultLogStorage.this.checkSelf();
			}

		}, 1, 5, TimeUnit.MINUTES);

		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				DefaultLogStorage.this.storeCheckpoint.flush();
			}

		}, this.getStoreConfig().getStoreCheckPointFlushInterval(), 1000 * 30, TimeUnit.MILLISECONDS);
	}

	private void checkSelf() {
		List<DefaultDataBase> dblist = this.getDbList();
		for (DefaultDataBase dataBase : dblist) {
			dataBase.checkSelf();
		}
	}

	/**
	 * 启动服务后，在存储根目录创建临时文件，类似于 UNIX VI编辑工具
	 *
	 * @throws IOException
	 */
	private void createTempFile() throws IOException {
		String fileName = this.storeConfig.getAbortFile();
		File file = new File(fileName);
		MapedFile.ensureDirOK(file.getParent());
		boolean result = file.createNewFile();
		logger.info(fileName + (result ? " create OK" : " already exists"));
	}

	private void unmapFilesPeriodically() {
		this.cleanPhyMappedFileService.run();
	}
	
	private void cleanFilesPeriodically() {
		this.cleanPhysicLogService.run();
	
		this.cleanIndexDBService.run();
	}

	private void deleteFile(final String fileName) {
		File file = new File(fileName);
		boolean result = file.delete();
		logger.info(fileName + (result ? " delete OK" : " delete Failed"));
	}

	@Override
	public String getLogStorageDirPath(int groupIdx) {
		if (groupIdx >= this.dbList.size()) {
			return "";
		}
		return this.dbList.get(groupIdx).getDbRootPath();
	}

	@Override
	public int get(int groupIdx, long instanceId, JavaOriTypeWrapper<byte[]> valueWrap) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}
		byte[] value = this.dbList.get(groupIdx).get(instanceId);
		valueWrap.setValue(value);
		if (value == null) {
			return 1;
		}
		return 0;
	}

	@Override
	public int put(WriteOptions writeOptions, int groupIdx, long instanceID, byte[] value, WriteState writeState) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}
		try {
			this.dbList.get(groupIdx).put(writeOptions, instanceID, value, writeState);
		} catch (Exception e) {
			e.printStackTrace();
			return -2;
		}
		return 0;
	}

	@Override
	public int delOne(WriteOptions writeOptions, int groupIdx, long instanceId) {
		return 0;
	}

	@Override
	public int delExpire(WriteOptions writeOptions, int groupIdx, long maxInstanceId) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}
		this.dbList.get(groupIdx).delExpire(writeOptions, maxInstanceId);
		return 0;
	}

	@Override
	public int getMaxInstanceID(int groupIdx, JavaOriTypeWrapper<Long> instanceIDWrap) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}
		long maxInstanceId = this.dbList.get(groupIdx).getMaxInstanceId();
		instanceIDWrap.setValue(maxInstanceId);
		if (maxInstanceId == DefaultDataBase.MINCHOSEN_KEY) {
			return 1;
		}
		return 0;
	}

	@Override
	public int setMinChosenInstanceID(WriteOptions writeOptions, int groupIdx, long minInstanceId) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}
		this.dbList.get(groupIdx).setMinChosenInstanceId(writeOptions, minInstanceId);
		return 0;
	}

	@Override
	public long getMinChosenInstanceID(int groupIdx) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}

		return this.dbList.get(groupIdx).getMinChosenInstanceId();
	}

	@Override
	public int clearAllLog(int groupIdx) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}
		try {
			this.dbList.get(groupIdx).clearAllLog();
		} catch (Exception e) {
			logger.error("clearAllLog error", e);
			return -1;
		}
		return 0;
	}

	@Override
	public int setSystemVariables(WriteOptions writeOptions, int groupIdx, byte[] buffer) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}
		this.dbList.get(groupIdx).setSystemVariables(writeOptions, buffer);
		return 0;
	}

	@Override
	public byte[] getSystemVariables(int groupIdx) {
		return this.dbList.get(groupIdx).getSystemVariables();
	}

	@Override
	public int setMasterVariables(WriteOptions writeOptions, int groupIdx, byte[] buffer) {
		if (groupIdx >= this.dbList.size()) {
			return -2;
		}
		this.dbList.get(groupIdx).setMasterVariables(writeOptions, buffer);
		return 0;
	}

	@Override
	public byte[] getMasterVariables(int groupIdx) {
		return this.dbList.get(groupIdx).getMasterVariables();
	}

	public StoreConfig getStoreConfig() {
		return storeConfig;
	}

	public int getPayLoadStartOffset() {
		return PhysicLog.getPayLoadStartOffset();
	}

	@Override
	public void deleteOneIndex(int groupId, long instanceId) {
		this.dbList.get(groupId).delIndex(new WriteOptions(), instanceId);
	}

	@Override
	public void deleteExpireIndex(int groupId, long maxInstanceId) {
		this.dbList.get(groupId).delIndexExpire(new WriteOptions(), maxInstanceId);
	}

	public List<DefaultDataBase> getDbList() {
		return dbList;
	}

	public TransientStorePool getTransientStorePool() {
		return transientStorePool;
	}

	public AllocateMapedFileService getAllocateMapedFileService() {
		return allocateMapedFileService;
	}


	public StoreCheckpoint getStoreCheckpoint() {
		return storeCheckpoint;
	}

	public FlushPhysicLogService getFlushRealTimeService() {
		return flushPhysicLogService;
	}

	public CommitPhysicLogService getCommitRealTimeService() {
		return commitRealTimeService;
	}

	public StoreStatus getStoreStatus() {
		return storeStatus;
	}

	public GroupSMInfo getGroupSMInfo(int groupId) {
		return this.options.getGroupSMInfoList().get(groupId);
	}

	public DataBase getDataBase(int groupId) {
		return dbList.get(groupId);
	}

	public Options getOptions() {
		return options;
	}
}
