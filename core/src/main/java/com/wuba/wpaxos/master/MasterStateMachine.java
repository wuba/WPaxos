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
package com.wuba.wpaxos.master;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.MasterChangeCallback;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.proto.MasterOperator;
import com.wuba.wpaxos.proto.MasterVariables;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.storemachine.InsideSM;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.OtherUtils;

/**
 * master state machine
 */
public class MasterStateMachine implements InsideSM {
	private final Logger logger = LogManager.getLogger(MasterStateMachine.class); 
	private int myGroupIdx;
	private long myNodeID;
	private MasterVariablesStore mVStore;
	private long masterNodeID;
	private long masterVersion;
	private int leaseTime;
	private long absExpireTime;
	private ReentrantLock mslock = new ReentrantLock();
	private MasterChangeCallback masterChangeCallback;
	
	public MasterStateMachine(LogStorage logStorage, long myNodeId, int groupIdx, MasterChangeCallback masterChangeCallback) {
		this.mVStore = new MasterVariablesStore(logStorage);
		this.masterChangeCallback = masterChangeCallback;
		this.myGroupIdx = groupIdx;
		this.myNodeID = myNodeId;
		this.masterNodeID = 0;
		this.masterVersion = -1;
		this.leaseTime = 0;
		this.absExpireTime = 0;
	}
	
	@Override
	public int getSMID() {
		return Def.MASTER_V_SMID;
	}
	
	@Override
	public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
		MasterOperator masterOper = new MasterOperator();
		try {
			masterOper.parseFromBytes(paxosValue, paxosValue.length);	
		} catch(Exception e) {
			logger.error("masterOper data wrong.", e);
			return false;
		}
		
		if (masterOper.getOperator() == MasterOperatorType.MasterOperatorType_Complete.getType()) {
			Long pabsMasterTimeout = null;
			if (smCtx != null) {
				pabsMasterTimeout = (Long) smCtx.getpCtx();
			}
			
			long absMasterTimeout = (pabsMasterTimeout != null) ? pabsMasterTimeout : 0;
			logger.debug("absmaster timeout {}.", absMasterTimeout);
			
			int ret = learnMaster(instanceID, masterOper, absMasterTimeout);
			if (ret != 0) {
				return false;
			}
		} else {
			logger.error("unknown op {}.", masterOper.getOperator());
			return true;
		}
		
		return true;
	}
	
	@Override
	public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
		return true;
	}
	
	@Override
	public long getCheckpointInstanceID(int groupIdx) {
		return this.masterVersion;
	}
	
	@Override
	public int lockCheckpointState() {
		return 0;
	}
	
	@Override
	public int getCheckpointState(int groupIdx, JavaOriTypeWrapper<String> dirPath, List<String> fileList) {
		return 0;
	}
	
	@Override
	public void unLockCheckpointState() {
	}
	
	@Override
	public int loadCheckpointState(int groupIdx, String checkpointTmpFileDirPath, List<String> fileList, long checkpointInstanceID) {
		return 0;
	}
	
	@Override
	public byte[] beforePropose(int groupIdx, byte[] sValue) {
		this.mslock.lock();
		try {
			MasterOperator masterOperator = new MasterOperator();
			try {
				masterOperator.parseFromBytes(sValue, sValue.length);
			} catch(Exception e) {
				e.printStackTrace();
				return sValue;
			}
			
			masterOperator.setLastversion(this.masterVersion);
			byte[] buf = null;
			try {
				buf = masterOperator.serializeToBytes();
			} catch(Exception e) {
				logger.error("masterOperator serialize error.", e);
			}
			
			if (buf != null) {
				sValue = buf;
			}	
		} finally {
			this.mslock.unlock();
		}
		
		return sValue;
	}
	
	@Override
	public boolean needCallBeforePropose() {
		return true;
	}
	
	@Override
	public byte[] getCheckpointBuffer() {
		this.mslock.lock();
		
		byte[] cpBuffer = null;
		MasterVariables variables = new MasterVariables();
		try {
			if (this.masterVersion == -1) {
				return cpBuffer;
			}
			
			variables.setMasterNodeID(this.masterNodeID);
			variables.setVersion(this.masterVersion);
			variables.setLeaseTime(this.leaseTime);
		} finally {
			this.mslock.unlock();
		}
		
		try {
			cpBuffer = variables.serializeToBytes();
		} catch(Exception e) {
			logger.error("Variables.serialize failed.", e);
		}
		
		return cpBuffer;
	}
	
	@Override
	public UpdateCpRet updateByCheckpoint(byte[] sCPBuffer) {
		UpdateCpRet updateCpRet = new UpdateCpRet(false, 0);
		
		if (sCPBuffer == null || sCPBuffer.length == 0) {
			return updateCpRet;
		}
		
		MasterVariables variables = new MasterVariables();
		try {
			variables.parseFromBytes(sCPBuffer, sCPBuffer.length);
		} catch(Exception e) {
			logger.error("variables parseFromArray failed, bufferlen {}.", sCPBuffer.length, e);
			updateCpRet.setRet(-1);
			return updateCpRet;
		}
		boolean isImMaster = false;
		mslock.lock();
		
		try {
			if (variables.getVersion() <= this.masterVersion && this.masterVersion != -1) {
				logger.debug("lag checkpoint, no need update, cp.version {} now.version {}.", variables.getVersion(), this.masterVersion);
				return updateCpRet;
			}
			
			int ret = updateMasterToStore(variables.getMasterNodeID(), variables.getVersion(), variables.getLeaseTime());
			if (ret != 0) {
				updateCpRet.setRet(-1);
				return updateCpRet;
			}
			
			logger.debug("OK, cp.version {} cp.masternodeid {} old.version {} old.masternodeid {}.",
					variables.getVersion(), variables.getMasterNodeID(), this.masterVersion, this.masterNodeID);
			
			this.masterVersion = variables.getVersion();
			
			if (variables.getMasterNodeID() == this.myNodeID) {
				this.masterNodeID = 0;
				this.absExpireTime = 0;
				isImMaster = true;
			} else {
				if (this.masterNodeID != variables.getMasterNodeID()) {
					updateCpRet.setChange(true);
				}
				this.masterNodeID = variables.getMasterNodeID();
				this.absExpireTime = OtherUtils.getSystemMS() + variables.getLeaseTime();
			}
		} catch(Exception e) {
			logger.error("updateByCheckpoint error.", e);
		} finally {
			this.mslock.unlock();
		}

		//if (updateCpRet.isChange()) {
			if (this.masterChangeCallback != null) {
				this.masterChangeCallback.callback(this.myGroupIdx, updateCpRet.isChange(),isImMaster,true);
			}
		//}

		return updateCpRet;
 	}

	public int init() {
		MasterVariables variables = this.mVStore.read(this.myGroupIdx);
		if (variables == null) {
			logger.error("master variables read from store failed.");
		} else {
			this.masterVersion = variables.getVersion();

			if (variables.getMasterNodeID() == this.masterNodeID) {
				this.masterNodeID = 0;
				this.absExpireTime = 0;
			} else {
				this.masterNodeID = variables.getMasterNodeID();
				this.absExpireTime = OtherUtils.getSystemMS() + variables.getLeaseTime();
			}
		}

		logger.debug("OK, master nodeid {} version {} expiretime {}.", this.masterNodeID, this.masterVersion, this.absExpireTime);
		return 0;
	}

	public int learnMaster(long instanceID, MasterOperator masterOper, long absMasterTimeout) {
		this.mslock.lock();

		try {
			logger.debug("my last version {}, other last version {} this version {} instanId {}.",
					this.masterVersion, masterOper.getLastversion(), masterOper.getVersion(), instanceID);

			if (masterOper.getLastversion() != 0 && instanceID > this.masterVersion && masterOper.getLastversion() != this.masterVersion) {
				logger.error("other last version {} not same to my last version {}, instanceid {}.",
						masterOper.getLastversion(), this.masterVersion, instanceID);

				logger.error("try to fix, set my master version {} as other last version {}, instanceid {}.",
						this.masterVersion, masterOper.getLastversion(), instanceID);
				this.masterVersion = masterOper.getLastversion();
			}

			if (masterOper.getVersion() != this.masterVersion) {
				logger.debug("version conflit, op version {} now master version {}", masterOper.getVersion(), this.masterVersion);
				return 0;
			}

			int ret = updateMasterToStore(masterOper.getNodeID(), instanceID, masterOper.getTimeout());
			if (ret != 0) {
				logger.error("updateMasterToStore failed, ret {}.", ret);
				return -1;
			}

			boolean masterChange = false;
			boolean isImMaster = false;
			if (this.masterNodeID != masterOper.getNodeID()) {
				masterChange = true;
			}

			this.masterNodeID = masterOper.getNodeID();
			if (this.masterNodeID == this.myNodeID) {
				// self be master
				// use local abstimeout
				this.absExpireTime = absMasterTimeout;
				isImMaster=true;
				Breakpoint.getInstance().getMasterBP().successBeMaster(this.myGroupIdx);
				logger.debug("Be master success, absexpiretime {} groupid {}.", this.absExpireTime,myGroupIdx);
			} else {
				// other be master
				// use new start timeout
				this.absExpireTime = OtherUtils.getSystemMS() + masterOper.getTimeout();
				Breakpoint.getInstance().getMasterBP().otherBeMaster(this.myGroupIdx);
				logger.debug("Other be master, absexpiretime {} groupid {}.", this.absExpireTime,myGroupIdx);
			}
			
			this.leaseTime = masterOper.getTimeout();
			this.masterVersion = instanceID;
			
			if (this.masterChangeCallback != null) {
				this.masterChangeCallback.callback(this.myGroupIdx, masterChange,isImMaster,false);
			}
			
			logger.debug("OK, masternodeid {} version {} abstimeout {}.", this.masterNodeID, this.masterVersion, this.absExpireTime);
			
			return 0;
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			this.mslock.unlock();
		}

		return -1;
	}

	public void setAbsExpireTime(long absExpireTime) {
		this.absExpireTime = absExpireTime;
	}

	public long getMaster() {
		if (OtherUtils.getSystemMS() >= this.absExpireTime) {
			return 0;
		}
		
		return this.masterNodeID;
	}
	
	public MasterInfo getMasterWithVersion() {
		MasterInfo masterInfo = new MasterInfo();
		safeGetMaster(masterInfo);
		return masterInfo;
	}
	
	public boolean isIMMaster() {
		long masterNodeID = getMaster();
		return masterNodeID == this.myNodeID;
	}
	
	public boolean noMaster() {
		long masterNodeID = getMaster();
		return masterNodeID == 0;
	}
	
	public int updateMasterToStore(long masterNodeID, long version, int leaseTime) {
		MasterVariables variables = new MasterVariables();
		variables.setMasterNodeID(masterNodeID);
		variables.setVersion(version);
		variables.setLeaseTime(leaseTime);
		
		WriteOptions writeOptions = new WriteOptions(true);
		return this.mVStore.write(writeOptions, this.myGroupIdx, variables);
	}
	
	public void safeGetMaster(MasterInfo masterInfo) {
		mslock.lock();
		try{
			if (OtherUtils.getSystemMS() >= this.absExpireTime) {
				masterInfo.setMasterNodeID(0);
			} else {
				masterInfo.setMasterNodeID(this.masterNodeID);
			}

			masterInfo.setMasterVersion(this.masterVersion);
		}finally{
			mslock.unlock();
		}
	}
	
	public static byte[] makeOpValue(long nodeID, long version, int timeout, MasterOperatorType op) {
		MasterOperator masterOper = new MasterOperator();
		masterOper.setNodeID(nodeID);
		masterOper.setVersion(version);
		masterOper.setTimeout(timeout);
		masterOper.setOperator(op.getType());
		masterOper.setSid(OtherUtils.fastRand());
		
		try {
			return masterOper.serializeToBytes();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID) {
	}
}

enum MasterOperatorType
{
    MasterOperatorType_Complete(1);
    
    private int type;
    private MasterOperatorType(int type) {
    	this.type = type;
    }
    
	public int getType() {
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}
};
