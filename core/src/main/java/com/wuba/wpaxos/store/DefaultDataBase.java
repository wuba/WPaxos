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
import org.apache.logging.log4j.core.Logger;

import com.wuba.wpaxos.comm.GroupSMInfo;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.comm.enums.IndexType;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.store.db.FileIndexDB;
import com.wuba.wpaxos.store.db.IndexDB;
import com.wuba.wpaxos.store.db.LevelDBIndex;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.ByteConverter;
import com.wuba.wpaxos.utils.FileUtils;
import com.wuba.wpaxos.utils.Time;

import java.io.File;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * default database realize
 */
public class DefaultDataBase implements DataBase {
	private static final Logger logger = (Logger) LogManager.getLogger(DefaultDataBase.class);
	public static final long MINCHOSEN_KEY = Long.MAX_VALUE - 1;
	public static final long SYSTEMVARIABLES_KEY = Long.MAX_VALUE - 2;
	public static final long MASTERVARIABLES_KEY = Long.MAX_VALUE - 3;
	private volatile long maxInstanceId = -1;
	private final DefaultLogStorage defaultLogStorage;
	private final StoreConfig storeConfig;
	private PhysicLog valueStore;
	private IndexDB indexdb;
	private boolean hasInit;
	private String dbRootPath;
	private String localDbPath;
	private String logPath;
	private int myGroupIdx;

	public DefaultDataBase(DefaultLogStorage defaultLogStorage, StoreConfig storeConfig) {
		this.defaultLogStorage = defaultLogStorage;
		this.storeConfig = storeConfig;
		this.valueStore = null;
		this.hasInit = false;
		this.myGroupIdx = -1;
	}

	@Override
	public boolean init(String dbPath, int myGroupIdx) {
		if (this.hasInit) {
			return true;
		}

		File fileGroup = new File(dbPath);
		if (!fileGroup.exists()) {
			fileGroup.mkdirs();
		}

		this.myGroupIdx = myGroupIdx;
		this.dbRootPath = dbPath;

		this.logPath = this.dbRootPath + File.separator + "physiclog";
		this.localDbPath = this.dbRootPath + File.separator + "localdb";
		this.valueStore = new PhysicLog(this.defaultLogStorage, myGroupIdx, logPath, storeConfig, defaultLogStorage.getAllocateMapedFileService());
		if (defaultLogStorage.getOptions().getIndexType().getType() == IndexType.PHYSIC_FILE.getType()) {
			this.indexdb = new FileIndexDB(defaultLogStorage,myGroupIdx,localDbPath);
		} else {
			this.indexdb = new LevelDBIndex(myGroupIdx, localDbPath);
		}
		boolean result = true;
		result = result && this.valueStore.load();
		result = result && this.indexdb.init();
		byte[] idBytes = this.indexdb.getMaxInstanceID();

		if (idBytes != null) {
			this.maxInstanceId = ByteConverter.bytesToLongLittleEndian(idBytes);
			logger.info("DefaultDataBase init maxinstanceID {},  groupIdx {}.", this.maxInstanceId, myGroupIdx);
		} else {
			this.maxInstanceId = MINCHOSEN_KEY;
			logger.info("DefaultDataBase init maxinstanceID null.");
		}
		try {
			result = result && recover();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		
		if (result) {
			this.hasInit = true;
		}
		
		return result;
	}

	private boolean recover() throws Exception {
		boolean lastExitOK = !this.isTempFileExist();
		logger.info("last shutdown {}", (lastExitOK ? "normally" : "abnormally"));
		if (defaultLogStorage.getOptions().getIndexType().getType() == IndexType.PHYSIC_FILE.getType()) {
			FileIndexDB fileIndexDB = (FileIndexDB) indexdb;
			fileIndexDB.recover();
		}
		if (lastExitOK) {
			this.valueStore.recoverNormally();
		} else {
			this.valueStore.recoverAbnormally();
		}

		long maxInstanceId = this.getMaxInstanceId();
		long minInstanceId = this.getMinChosenInstanceId();
		logger.info("DefaultDataBase recover maxInstanceId {}, minInstanceId {}, groupId {}.", maxInstanceId, minInstanceId, this.myGroupIdx);
		
		if (maxInstanceId == MINCHOSEN_KEY) {
			return true;
		}

		// rebuildindex 非空检测，选出commitlog不为空的最大的instance id
		boolean rebuildIndexCheckResult = false;
		long maxCheckInstanceId = maxInstanceId;
				
		for (long i = maxCheckInstanceId; i >= minInstanceId; i--) {
			byte[] data = this.get(i);
			if (data != null) {
				logger.info("rebuild index set max instance id : {}, groupId : {}.", i, this.myGroupIdx);
				this.setMaxInstanceId(i);
				rebuildIndexCheckResult = true;
				break;
			} else {
				this.indexdb.deleteOneIndex(i);
				logger.warn("rebuild index delete localdb index [ {} ], groupId : {}.", i, this.myGroupIdx);
			}
		}
		
		// rebuild to max
		FileID fileID = this.getFileIDByInstanceID(this.getMaxInstanceId());
		TreeMap<Long, FileID> fileIdMap = this.valueStore.getFileIds(fileID);
		if (fileIdMap != null && !fileIdMap.isEmpty()) {
			for (Entry<Long, FileID> entry : fileIdMap.entrySet()) {
				long nowInstanceID = entry.getKey();
				FileID nowFileID = entry.getValue();
				if (nowInstanceID > this.getMaxInstanceId()) {
					this.setMaxInstanceId(nowInstanceID);
					this.putToIndexDB(new WriteOptions(false), nowInstanceID, nowFileID);
					rebuildIndexCheckResult = true;
				}
			}
		}

		if ((maxCheckInstanceId > minInstanceId) && !rebuildIndexCheckResult) {
			throw new Exception("rebuild index check result failed");
		}

		FileID fileId = this.getFileIDByInstanceID(this.getMaxInstanceId());
		if (fileId == null) {
			logger.warn("maxInstanceId {}, value null.", this.getMaxInstanceId());
		} else {
			this.valueStore.correctMaxPhyOffset(fileId.getOffset() + fileId.getSize());
		}

		this.indexdb.correctMaxInstanceID(this.getMaxInstanceId());

		return true;
	}

	@Override
	public boolean isAvailable() {
		return this.hasInit;
	}

	private boolean isTempFileExist() {
		String fileName = this.storeConfig.getAbortFile();
		File file = new File(fileName);
		return file.exists();
	}

	@Override
	public void clearAllLog() {
		byte[] systemVariables = getSystemVariables();
		byte[] masterVariables = getMasterVariables();

		this.hasInit = false;
		this.valueStore.destroy();
		this.valueStore = null;
		this.indexdb.destroy();
		this.indexdb = null;
		//删除bak目录，将原存储目录改为bak目录
		String bakPath = this.dbRootPath + ".bak";
		FileUtils.deleteDir(bakPath);
		File dbFile = new File(this.dbRootPath);
		dbFile.renameTo(new File(bakPath));
		logger.info("rename index log success, ori=" + this.dbRootPath + ", bakPath=" + bakPath);
		logger.info("destroy paxos log and indexdb success, groupId {}.", this.myGroupIdx);

		//初始化并设置原有变量
		init(this.dbRootPath, myGroupIdx);
		WriteOptions writeOptions = new WriteOptions(true);
		setSystemVariables(writeOptions, systemVariables);
		if (masterVariables != null) {
			setMasterVariables(writeOptions, masterVariables);
		}
	}

	public int getMyGroupIdx() {
		return myGroupIdx;
	}

	public void setMyGroupIdx(int myGroupIdx) {
		this.myGroupIdx = myGroupIdx;
	}

	@Override
	public byte[] get(long instanceId) {		
		FileID fileId = getFileIDByInstanceID(instanceId);

		if (fileId == null) {
			return null;
		}
		return this.valueStore.getData(fileId);
	}

	/**
	 * @param writeOptions
	 * @param instanceId
	 * @param value
	 */
	@Override
	public int put(WriteOptions writeOptions, long instanceId, byte[] value, WriteState writeState) {		
		long start = System.currentTimeMillis();
		PutDataResult pdr = this.valueStore.appendData(writeOptions, instanceId, value, writeState);
		long cost = System.currentTimeMillis() - start;
		if (cost > 100) {
			logger.info("TRACE put valueStore cost :" + cost);
		}

		if (pdr == null || pdr.getPutDataStatus() != PutDataStatus.PUT_OK) {
			logger.error("put value failed, status=" + pdr.getPutDataStatus());
			return -1;
		}
		AppendDataResult appendDataResult = pdr.getAppendDataResult();
		FileID fileID = new FileID(appendDataResult.getWroteOffset(), appendDataResult.getCrc32(), appendDataResult.getWroteBytes());
		this.putToIndexDB(writeOptions, instanceId, fileID);

		return 0;
	}

	@Override
	public long getMaxInstanceId() {
		return this.maxInstanceId;
	}

	@Override
	public void setMinChosenInstanceId(WriteOptions writeOptions, long instanceId) {
		this.indexdb.setMinChosenInstanceID(writeOptions ,instanceId);
	}

	@Override
	public long getMinChosenInstanceId() {
		if (this.indexdb != null) {
			byte[] idBytes = this.indexdb.getMinChosenInstanceID();
			if (idBytes != null) {
				return ByteConverter.bytesToLongLittleEndian(idBytes);
			}
		}

		return 0;
	}

	@Override
	public void updateCpByMinChosenInstanceID(int groupId, long newMinChosenInstanceId) {
		GroupSMInfo groupSMInfo = this.defaultLogStorage.getGroupSMInfo(groupId);
		List<StateMachine> smlist = groupSMInfo.getSmList();
		for (StateMachine sm : smlist) {
			long cpInstanceId = sm.getCheckpointInstanceID(groupId);

			//如果当前checkpoint消息小于min chosen instance id，更新checkpoint
			//此时如果sm并发执行了其他操作，更新checkpoint，checkpoint被回归到min chosen instance id，由于checkpoint只会在learn和启动检测阶段使用，没有影响
			if (cpInstanceId < newMinChosenInstanceId) {
				sm.executeForCheckpoint(groupId, newMinChosenInstanceId, null);
				logger.info("update checkpoint by min chosen instance id, groupid={}, instanceid={}, smid={}.", groupId, newMinChosenInstanceId, sm.getSMID());
			}
		}
	}

	@Override
	public void setSystemVariables(WriteOptions writeOptions, byte[] buffer) {
		this.indexdb.setSystemvariables(writeOptions, buffer);
	}

	@Override
	public byte[] getSystemVariables() {
		return this.indexdb.getSystemvariables();
	}

	@Override
	public void setMasterVariables(WriteOptions writeOptions, byte[] buffer) {
		this.indexdb.setMastervariables(writeOptions, buffer);
	}

	@Override
	public byte[] getMasterVariables() {
		return this.indexdb.getMastervariables();
	}

	@Override
	public void delExpire(WriteOptions writeOptions, long maxInstanceId) {
		if (this.indexdb != null) {
			FileID index = indexdb.getIndex(maxInstanceId);
			if (index != null) {
				valueStore.deleteExpiredFile(index.getOffset());
			}
			delIndexExpire(writeOptions,maxInstanceId);
		}
	}

	@Override
	public void delIndex(WriteOptions writeOptions, long instanceId) {
		if (this.indexdb != null) {
			indexdb.deleteOneIndex(instanceId);
		}
	}

	@Override
	public void delIndexExpire(WriteOptions writeOptions, long maxInstanceId) {
		if (this.indexdb != null) {
			indexdb.deleteExpire(writeOptions,maxInstanceId);
		}
	}

	public FileID getFileIDByInstanceID(long instanceId) {
		try {
			if (this.indexdb != null) {
				FileID fileID = this.indexdb.getIndex(instanceId);
				return fileID;
			}
		} catch (Exception e) {
			logger.error("getFromIndexDB failed.", e);
		}
		return null;
	}

	private void putToIndexDB(WriteOptions writeOptions, long instanceId, FileID fileID) {
		long start = Time.getSteadyClockMS();
		try {
			this.indexdb.putIndex(writeOptions, instanceId, fileID);
		} catch (Exception e) {
			logger.error("put to indexdb failed : ", e);
		}

		this.setMaxInstanceId(instanceId);
		long end = Time.getSteadyClockMS();
		Breakpoint.getInstance().getLogStorageBP().indexDBPutOK((int) (end - start), this.myGroupIdx, instanceId);
	}

	private void setMaxInstanceId(long instanceId) {
		//排除3个特殊instanceid
		if (instanceId == MINCHOSEN_KEY ||
				instanceId == SYSTEMVARIABLES_KEY ||
				instanceId == MASTERVARIABLES_KEY) {
			return;
		}

		//TODO 非线程安全!!!
		//设置最大instance id
		this.maxInstanceId = instanceId;
		this.indexdb.setMaxInstanceID(new WriteOptions(false), instanceId);
	}

	public String getDbRootPath() {
		return dbRootPath;
	}

	public IndexDB getIndexdb() {
		return indexdb;
	}

	public PhysicLog getValueStore() {
		return valueStore;
	}


	public void start() {
		if (defaultLogStorage.getOptions().getIndexType().getType() == IndexType.PHYSIC_FILE.getType()) {
			FileIndexDB fileIndexDB = (FileIndexDB) indexdb;
			fileIndexDB.start();
		}
	}

	public void shutDown() {
		if (defaultLogStorage.getOptions().getIndexType().getType() == IndexType.PHYSIC_FILE.getType()) {
			FileIndexDB fileIndexDB = (FileIndexDB) indexdb;
			fileIndexDB.shutdown();
		}
	}

	public void checkSelf() {
		if (valueStore != null) {
			valueStore.getMapedFileQueue().checkSelf();
		}
		if (defaultLogStorage.getOptions().getIndexType().getType() == IndexType.PHYSIC_FILE.getType()) {
			if (indexdb != null) {
				FileIndexDB fileIndexDB = (FileIndexDB) indexdb;
				fileIndexDB.getMapedFileQueue().checkSelf();
			}
		}
	}

	public void clearIndexCache() {
		((FileIndexDB) indexdb).clearIndexCache();
	}
}
















