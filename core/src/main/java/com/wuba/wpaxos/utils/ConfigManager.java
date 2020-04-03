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
package com.wuba.wpaxos.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public abstract class ConfigManager {
    private static final Logger logger = LogManager.getLogger(ConfigManager.class);

    public abstract String encode();


    public abstract String encode(final boolean prettyFormat);


    public abstract void decode(final String jsonString);


    public abstract String configFilePath();


    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            // 文件不存在，或者为空文件
            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            }
            else {
                this.decode(jsonString);
                logger.info("init {} OK", fileName);
                return true;
            }
        }
        catch (Exception e) {
        	logger.error("init " + fileName + " Failed, and try to init backup file", e);
            return this.loadBak();
        }
    }


    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                logger.info("init " + fileName + " OK");
                return true;
            }
        }
        catch (Exception e) {
        	logger.error("init " + fileName + " Failed", e);
            return false;
        }

        return true;
    }


    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            }
            catch (IOException e) {
            	logger.error("persist file Exception, " + fileName, e);
            }
        }
    }	
}
