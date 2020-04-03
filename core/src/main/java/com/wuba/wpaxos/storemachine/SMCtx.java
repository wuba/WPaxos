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
package com.wuba.wpaxos.storemachine;

/**
 * state machine context
 */
public class SMCtx {
	private int smId;
	private Object pCtx;
	
	public SMCtx() {
	}
	
	public SMCtx(int smId, Object pCtx) {
		super();
		this.smId = smId;
		this.pCtx = pCtx;
	}
	
	public int getSmId() {
		return smId;
	}
	
	public void setSmId(int smId) {
		this.smId = smId;
	}
	
	public Object getpCtx() {
		return pCtx;
	}
	
	public void setpCtx(Object pCtx) {
		this.pCtx = pCtx;
	}
}
