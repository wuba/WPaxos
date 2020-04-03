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
package com.wuba.wpaxos.comm;

import com.wuba.wpaxos.utils.OtherUtils;

public class InsideOptions {
	private boolean isLargeBufferMode;
	private boolean isIMFollower;
	private int groupCount;

	private static InsideOptions instance = new InsideOptions();

	private InsideOptions() {
		this.isLargeBufferMode = false;
		this.isIMFollower = false;
		this.groupCount = 1;
	}

	public static InsideOptions getInstance() {
		return instance;
	}

	public void setAsLargeBufferMode() {
		this.isLargeBufferMode = true;
	}

	public void setAsFollower() {
		this.isIMFollower = true;
	}

	public void setGroupCount(int groupCount) {
		this.groupCount = groupCount;
	}

	public int getMaxBufferSize() {
		if(this.isLargeBufferMode) {
			//50M
			return 52428800;
		} else {
			//10M
			return 10485760;
		}
	}

	public int getStartPrepareTimeoutMs() {
		if(this.isLargeBufferMode) {
			return 15000;
		} else {
			return 1500;
		}
	}

	public int getStartAcceptTimeoutMs() {
		if (isLargeBufferMode) {
	    	return 15000;
	    } else {
	    	return 1000;
	    }
	}

	public int getMaxPrepareTimeoutMs() {
		if (isLargeBufferMode) {
			return 90000;
	    } else {
			//TODO 暂时调整
			// return 8000;
			return 8000;
	    }
	}

	public int getMaxAcceptTimeoutMs() {
		if (isLargeBufferMode)  {
			return 90000;
		} else {
			//TODO 暂时调整
			// return 8000;
			return 8000;
		}
	}

	public int getMaxIOLoopQueueLen() {
		if (isLargeBufferMode) {
	        return 1024 / this.groupCount + 100;
	    } else {
	    	//TODO
	        return 10240 / this.groupCount + 10000;

	    }
	}

	public int getMaxQueueLen() {
		if (isLargeBufferMode) {
	        return 1024;
	    } else {
	        return 10240;
	    }
	}

	public int getAskforLearnInterval() {
		// 如果自己不是 follower 那么可以发送给 learn 请求节点更长时间的数据。
	    if (!this.isIMFollower) {
	        if (isLargeBufferMode) {
	            return 50000 + (OtherUtils.fastRand() % 10000);
	        } else {
	            return 2500 + (OtherUtils.fastRand() % 500);
	        }
	    } else {
	        if (isLargeBufferMode) {
	            return 30000 + (OtherUtils.fastRand() % 15000);
	        } else {
	            return 2000 + (OtherUtils.fastRand() % 1000);
	        }
	    }
	}

	public int getLearnerReceiverAckLead() {
		if (isLargeBufferMode) {
	        return 2;
	    } else {
	        return 4;
	    }
	}

	public int getLearnerSenderPrepareTimeoutMs() {
		if (isLargeBufferMode) {
	        return 6000;
	    } else {
	        return 5000;
	    }
	}

	public int getLearnerSenderAckTimeoutMs() {
		if (isLargeBufferMode) {
	        return 60000;
	    } else {
	        return 5000;
	    }
	}

	public int getLearnerSenderAckLead() {
		if (isLargeBufferMode) {
	        return 5;
	    } else {
	        return 21;
	    }
	}

	public int getTcpOutQueueDropTimeMs() {
		if (isLargeBufferMode) {
	        return 20000;
	    } else {
	        return 5000;
	    }
	}

	public int getLogFileMaxSize() {
		if (isLargeBufferMode) {
			//100M
	        return 524288000;
	    } else {
	    	//100M
	        return 104857600;
	    }
	}

	public int getTcpConnectionNonActiveTimeout() {
		if (isLargeBufferMode) {
	        return 600000;
	    } else {
	        return 60000;
	    }
	}

	public int getLearnerSenderSendQps() {
		if (isLargeBufferMode) {
	        return 10000 / this.groupCount;
	    } else {
	        return 100000 / this.groupCount;
	    }
	}

	public int getCleanerDeleteQps() {
		if (isLargeBufferMode) {
	        return 30000 / groupCount;
	    } else {
	        return 300000 / groupCount;
	    }
	}

}
