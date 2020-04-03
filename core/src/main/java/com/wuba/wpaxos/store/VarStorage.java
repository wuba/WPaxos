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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.wuba.wpaxos.utils.ByteConverter;
import com.wuba.wpaxos.utils.ConfigManager;
import com.wuba.wpaxos.utils.ThreadFactoryImpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * fileIndex type最新paxoslog状态元信息存储
 */
public class VarStorage extends ConfigManager {
	private static final Logger logger = LogManager.getLogger(VarStorage.class);

	private static String MINCHOSEN_KEY = "MINCHOSEN_KEY";
	private static String SYSTEMVARIABLES_KEY = "SYSTEMVARIABLES_KEY";
	private static String MASTERVARIABLES_KEY = "MASTERVARIABLES_KEY";
	private static String MAX_INSTANCEID_KEY = "MAX_INSTANCEID_KEY";

	private String storePath;
	private int groupId;
	private ConcurrentHashMap<String, byte[]> varMap = new ConcurrentHashMap<>();

	public VarStorage() {
	}

	public VarStorage(String storeRootPath, int groupId) {
		this.groupId = groupId;
		this.storePath = storeRootPath + File.separator + "varlog" + "-" + groupId;
	}

	public void start() {
		ScheduledExecutorService scheduledExcutorService = Executors.
				newSingleThreadScheduledExecutor(new ThreadFactoryImpl("VarStorageScheduledPersistThread" + groupId));
		scheduledExcutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					VarStorage.this.persist();
				} catch (Exception e) {
					logger.error("schedule persist VarStorage error. ", e);
				}
			}
		}, 10, 60, TimeUnit.SECONDS);
	}

	public void shutdown() {
		VarStorage.this.persist();
	}

	public void destroy() {
		this.varMap.clear();
	}

	@JSONField(deserialize = false, serialize = false)
	public void setMinChosenInstanceID(long instanceID) {
		varMap.put(MINCHOSEN_KEY, ByteConverter.longToBytesLittleEndian(instanceID));
	}

	@JSONField(deserialize = false, serialize = false)
	public byte[] getMinChosenInstanceID() {
		return varMap.get(MINCHOSEN_KEY);
	}

	@JSONField(deserialize = false, serialize = false)
	public void setSystemvariables(byte[] systemVariables) {
		if (systemVariables == null) {
			logger.error("var storage setSystemvariables is null");
			return;
		}
		varMap.put(SYSTEMVARIABLES_KEY, systemVariables);
	}

	@JSONField(deserialize = false, serialize = false)
	public byte[] getSystemvariables() {
		return varMap.get(SYSTEMVARIABLES_KEY);
	}

	@JSONField(deserialize = false, serialize = false)
	public void setMastervariables(byte[] masterVariables) {
		if (masterVariables == null) {
			logger.error("var storage setMastervariables masterVariables is null");
			return;
		}
		varMap.put(MASTERVARIABLES_KEY, masterVariables);
	}

	@JSONField(deserialize = false, serialize = false)
	public byte[] getMastervariables() {
		return varMap.get(MASTERVARIABLES_KEY);
	}

	@JSONField(deserialize = false, serialize = false)
	public void setMaxInstanceID(long maxInstanceID) {
		varMap.put(MAX_INSTANCEID_KEY, ByteConverter.longToBytesLittleEndian(maxInstanceID));
	}

	@JSONField(deserialize = false, serialize = false)
	public byte[] getMaxInstanceID() {
		return varMap.get(MAX_INSTANCEID_KEY);
	}

	@Override
	public String encode() {
		return this.encode(false);
	}

	@Override
	public String encode(boolean prettyFormat) {
		return JSONObject.toJSONString(this, prettyFormat);
	}

	@Override
	public void decode(String jsonString) {
		if (jsonString != null) {
			VarStorage obj = JSONObject.parseObject(jsonString, VarStorage.class);
			if (obj != null) {
				this.varMap = obj.getVarMap();
				logger.info("var storage decode varMap : {}.", this.varMap);
			}
		}
	}

	@Override
	public String configFilePath() {
		return this.storePath;
	}

	public String getStorePath() {
		return storePath;
	}

	public void setStorePath(String storePath) {
		this.storePath = storePath;
	}

	public ConcurrentHashMap<String, byte[]> getVarMap() {
		return varMap;
	}

}












