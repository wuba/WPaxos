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

import java.util.TreeSet;

public class Timer {
	
	private long nowTimerID = 1;
	
	private TreeSet<TimerObj> timerSet = new TreeSet<TimerObj>(); 
	
    public long getNowTimerID() {
		return nowTimerID;
	}

	public void setNowTimerID(long nowTimerID) {
		this.nowTimerID = nowTimerID;
	}

	public void addTimer(long absTime, JavaOriTypeWrapper<Long> timerID) {
		 addTimerWithType(absTime, 0, timerID);
    }
    
    public void addTimerWithType(long absTime, int type, JavaOriTypeWrapper<Long> timerID) {
    	this.nowTimerID++;
    	timerID.setValue(this.nowTimerID);
    	
    	TimerObj tobj = new TimerObj(timerID.getValue(), absTime, type);
    	this.timerSet.add(tobj);
    }

    public boolean popTimeout(TimerObj obj) {
        if (this.timerSet.isEmpty()) {
            return false;
        }

        TimerObj tObj = this.timerSet.first();
        long nowTimeMs = Time.getSteadyClockMS();
        if (tObj.getAbsTime() > nowTimeMs) {
            return false;
        }
        
        this.timerSet.pollFirst();
        obj.setTimerID(tObj.getTimerID());
        obj.setType(tObj.getType());
        return true;
    }

    public int getNextTimeout(int defaultTimeout) {
        if (this.timerSet.isEmpty()) {
            return defaultTimeout;
        }

        int nextTimeout = 0;

        TimerObj tObj = this.timerSet.first();
        long nowTimeMs = Time.getSteadyClockMS();/*为什么加锁？*/
        if (tObj.getAbsTime() > nowTimeMs) {
        	nextTimeout = (int)(tObj.getAbsTime() - nowTimeMs);
        }

        return nextTimeout;
    }
	
    public static class TimerObj implements Comparable<TimerObj>{
		long timerID;
		long absTime;
		int type;
		
		public TimerObj() {
		}
		
		public TimerObj(long timerID, long absTime, int type) {
			super();
			this.timerID = timerID;
			this.absTime = absTime;
			this.type = type;
		}

		public long getTimerID() {
			return timerID;
		}

		public void setTimerID(long timerID) {
			this.timerID = timerID;
		}

		public long getAbsTime() {
			return absTime;
		}

		public void setAbsTime(long absTime) {
			this.absTime = absTime;
		}

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}
		
		@Override
		public TimerObj clone() {
			TimerObj obj = new TimerObj(timerID, absTime, type);
			return obj;
		}

		@Override
		public int compareTo(TimerObj o) {
			if (this.absTime == o.absTime) {
				return (int) (this.timerID - o.timerID);
			}
			return (this.absTime - o.absTime) > 0 ? 1 : -1;
		}

		@Override
		public String toString() {
			return "TimerObj [timerID=" + timerID + ", absTime=" + absTime
					+ ", type=" + type + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (absTime ^ (absTime >>> 32));
			result = prime * result + (int) (timerID ^ (timerID >>> 32));
			result = prime * result + type;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TimerObj other = (TimerObj) obj;
			if (absTime != other.absTime)
				return false;
			if (timerID != other.timerID)
				return false;
			if (type != other.type)
				return false;
			return true;
		}		
	}
}
