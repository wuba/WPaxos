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

import java.util.ArrayList;
import java.util.List;

/**
 * Batch propose statemachine context
 */
public class BatchSMCtx {
	List<SMCtx> smCtxList = new ArrayList<SMCtx>();
	
	public BatchSMCtx() {
		this.smCtxList = new ArrayList<SMCtx>();
	}

	public List<SMCtx> getSmCtxList() {
		return smCtxList;
	}

	public void setSmCtxList(List<SMCtx> smCtxList) {
		this.smCtxList = smCtxList;
	}
	
	public SMCtx getByIndex(int idx) {
		if (idx >= this.smCtxList.size()) {
			return null; 
		}
		return this.smCtxList.get(idx);
	}
}
