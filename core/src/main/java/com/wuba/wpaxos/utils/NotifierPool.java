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

public class NotifierPool {
	
	private ConcurrentHashMap<Long, Notifier> notifierMap = new ConcurrentHashMap<Long, Notifier>();
	
	public Notifier getNotifier(Long id) {
		Notifier notifier = notifierMap.get(id);
		if(notifier != null) {
			return notifier;
		}
		
		synchronized (notifierMap) {
			notifier = notifierMap.get(id);
			if(notifier == null) {
				notifier = new Notifier();
				notifierMap.put(id, notifier);
			}
		}
		return notifier;
	}
}
