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

import java.io.File;

/**
 * 存储文件路径管理
 */
public class StorePathConfigHelper {

	public static String getStorePathPhysicLog(final String rootDir) {
        return rootDir + File.separator + "physiclog";
    }

    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }
    
	public static String getStorePathIndexDB(final String rootDir) {
        return rootDir + File.separator + "indexDB";
    }
	
	public static String getStorePathVarStore(final String rootDir) {
		return rootDir + File.separator + "varlog";
	}
	
	public static String getAbortFile(final String rootDir) {
		return rootDir + File.separator + "abort";
	}
}
