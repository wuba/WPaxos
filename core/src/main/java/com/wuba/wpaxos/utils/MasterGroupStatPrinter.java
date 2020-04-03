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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 统计并输出master group
 * @author Service Platform Architecture Team (spat@58.com)
 *
 */
public class MasterGroupStatPrinter {
	
	private static final Logger logger = LogManager.getLogger(MasterGroupStatPrinter.class); 
	
	private static volatile ConcurrentHashMap<Integer, Object> masterGroupSet = new ConcurrentHashMap<Integer, Object>();
	
	static {
		Thread t = new Thread(new PrintTask());
		t.setName("MasterGroupStatPrinter-PrintTask");
		t.start();
	}
	
	public static void put(Integer groupId) {
		masterGroupSet.put(groupId, new Object());
	}
	
	public static class PrintTask implements Runnable {

		@Override
		public void run() {
			while(true) {
				try {
					ConcurrentHashMap<Integer, Object> tmpSet = masterGroupSet;
					masterGroupSet = new ConcurrentHashMap<Integer, Object>();
					
					logger.info("master group " + tmpSet.keySet());
				} catch (Throwable e) {
					logger.error("PrintTask throws exception", e);
				} finally {
					Time.sleep(15000);
				}
			}
		}	
	}

}












