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
package com.wuba.wpaxos.store.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.utils.FileInfo;
import com.wuba.wpaxos.utils.ThreadRenameFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * store config load dynamic
 */
public class StoreConfigLoader {
	private static StoreConfigLoader fileLoad = new StoreConfigLoader();
	private String configPath;
	private FileInfo fileInfo = null;
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new ThreadRenameFactory("StoreConfigLoad Thread"));
	
	private StoreConfigLoader() {}
	
	public static StoreConfigLoader getInstance() {
		return fileLoad;
	}
	
	public void setFileInfo(String path) throws IOException {
		this.configPath = path;
		fileInfo = new FileInfo(new File(path));
	}

	public String getConfigPath() {
		return configPath;
	}

	public void setConfigPath(String configPath) {
		this.configPath = configPath;
	}

	public void start(DynamicConfig dynamicConfig) {
		scheduler.scheduleWithFixedDelay(new ConfigObserverJob(fileInfo, dynamicConfig), 1, 1, TimeUnit.MINUTES);
	}
}

class ConfigObserverJob implements Runnable {
	private static final Logger logger = LogManager.getLogger(ConfigObserverJob.class);
	private static FileInfo fInfo;
	private static DynamicConfig dynamicConfig;

	public ConfigObserverJob(FileInfo fi, DynamicConfig dynamicConf) {
		fInfo = fi;
		dynamicConfig = dynamicConf;
	}
	
	@Override
	public void run() {
		try {
			if (fInfo != null && dynamicConfig != null) {
				File f = new File(fInfo.getFilePath());
				if (f != null) {
					if (f.lastModified() != fInfo.getLastModifyTime()) {
						dynamicConfig.loadConfig();
						fInfo.setLastModifyTime(f.lastModified());
						logger.info("Store config is reload.");
					}
				}
			}
		} catch(Throwable th) {
			logger.error("", th);
		}
	}
}