/*
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

public class Time {
	
	/**
	 * 用于获取时钟时间
	 * @return
	 */
	public static long getTimestampMS() {
		return System.currentTimeMillis();
	}

	/**
	 * 用于时间统计
	 * @return
	 */
	public static long getSteadyClockMS() {
		//TODO CHECK
		//return System.nanoTime() / 10000000;
		return System.currentTimeMillis();
	}

	/**
	 * 
	 * @param timeMs  毫秒
	 * @throws InterruptedException 
	 */
    public static void sleep(int timeMs) {
    	try {
			Thread.sleep(timeMs);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
}
