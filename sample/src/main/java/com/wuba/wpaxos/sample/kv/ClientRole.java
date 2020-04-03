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
package com.wuba.wpaxos.sample.kv;

public class ClientRole {
	private static final int master = 0;
	private static final int slave = 1;
	private static final int leaner = 2;
	public static int role = 0;

	public static boolean isLeaner() {
		return role == leaner;
	}

	public static void setLeaner() {
		ClientRole.role = leaner;
	}

	public static boolean isMaster() {
		return role == master;
	}

	public static boolean isSlave() {
		return role == slave;
	}
	
	
}
