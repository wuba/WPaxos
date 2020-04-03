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

import com.alibaba.fastjson.JSON;

public class KVOPValue {
	private String key;
	private String value;
	private int op;

	public KVOPValue() {
	}

	public KVOPValue(String key, String value, int op) {
		this.key = key;
		this.value = value;
		this.op = op;
	}

	public KVOPValue(String key, int op) {
		this.key = key;
		this.op = op;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getOp() {
		return op;
	}

	public void setOp(int op) {
		this.op = op;
	}

	public byte[] toBytes() {
		return JSON.toJSONBytes(this);
	}

	public static KVOPValue fromBytes(byte[] bs){
		return JSON.parseObject(bs, KVOPValue.class);
	}
}
