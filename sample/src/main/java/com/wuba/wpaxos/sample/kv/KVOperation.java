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

public enum KVOperation {
	WRITE(0,"写"),READ(1,"读"),DELETE(2,"删除");
	private int op;
	private String des;

	KVOperation(int op, String des) {
		this.op = op;
		this.des = des;
	}

	public int getOp() {
		return op;
	}

	public String getDes() {
		return des;
	}
}
