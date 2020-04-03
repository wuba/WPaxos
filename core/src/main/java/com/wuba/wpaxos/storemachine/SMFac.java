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

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.CommitCtx;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.proto.BatchPaxosValue;
import com.wuba.wpaxos.proto.PaxosValue;
import com.wuba.wpaxos.utils.ByteConverter;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * statemachines controller
 */
public class SMFac {
	private final Logger logger = LogManager.getLogger(SMFac.class); 
	private List<StateMachine> smList = new LinkedList<StateMachine>();
	private final int myGroupIdx;
	
	public SMFac(int myGroupIdx) {
		this.myGroupIdx = myGroupIdx;
	}
	
	public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
		try {
			if (paxosValue == null) {
				logger.error("Value wrong, instanceID {} paxosValue null.", instanceID);
				return true;
			}
			if (paxosValue.length < 4) {
				logger.error("Value wrong, intanceid {} size {}.", instanceID, paxosValue.length);
				return true;
			}
			
			int smID = 0;
			smID = ByteConverter.bytesToIntLittleEndian(paxosValue, 0); //TODO docheck
			
			if (smID == 0) {
				logger.info("Value no need to do sm, just skip, instanceid {}.", instanceID);
				return true;
			}
			
			int bodyLen = paxosValue.length - 4;
			byte[] bodyValue = new byte[bodyLen];
			System.arraycopy(paxosValue, 4, bodyValue, 0, bodyLen);
			if (smID == Def.BATCH_PROPOSE_SMID) {
				BatchSMCtx batchSMCtx = null;
				if (smCtx != null && smCtx.getpCtx() != null) {
					batchSMCtx = (BatchSMCtx) smCtx.getpCtx();
				}
				return batchExecute(groupIdx, instanceID, bodyValue, batchSMCtx);
			} else {
				return doExecute(groupIdx, instanceID, bodyValue, smID, smCtx);
			}
		} catch(Exception e) {
			logger.error("SMFac execute failed.", e);
		}
		return false;
	}
	
	public boolean batchExecute(int groupIdx, long instanceID, byte[] bodyValue, BatchSMCtx batchSMCtx) {
		try {
	 		BatchPaxosValue batchPaxosValue = new BatchPaxosValue();
	 		try  {
	 			batchPaxosValue.parseFromBytes(bodyValue, bodyValue.length);
	 		} catch(Exception e) {
	 			logger.error("parseFromArray failed, valuesize {}.", bodyValue.length);
	 			e.printStackTrace();
	 		}
	 		
	 		if (batchSMCtx != null) {
	 			if (batchSMCtx.getSmCtxList().size() != batchPaxosValue.getBatchSize()) {
	 				logger.error("values size {} not equal to smctx size {}.", batchPaxosValue.getBatchSize(), batchSMCtx.getSmCtxList().size());
	 				return false;
	 			}
	 		}
	 		
	 		for (int i = 0; i < batchPaxosValue.getBatchSize(); i++) {
	 			PaxosValue value = batchPaxosValue.getByIndex(i);
	 			SMCtx smCtx = batchSMCtx != null ? batchSMCtx.getByIndex(i) : null;
	 			boolean executeSucc = doExecute(groupIdx, instanceID, value.getValue(), value.getSmID(), smCtx);
	 			if (!executeSucc) {
	 				return false;
	 			}
	 		}
			return true;
		} catch(Exception e) {
			logger.error("SMFac execute failed.", e);
		}
		
		return false;
	}
	
	public boolean doExecute(int groupIdx, long instanceID, byte[] bodyValue, int smID, SMCtx smCtx) {
		try {
			if (smID == 0) {
				logger.info("Value no need to do sm, just skip, instanceid {}.", instanceID);
				return true;
			}
			
			if (this.smList.size() == 0) {
				logger.info("No any sm, need wait sm, instanceID {}.", instanceID);
				return false;
			}
			
			for (StateMachine sm : this.smList) {
				if (sm.getSMID() == smID) {
					long startTimestamp = System.currentTimeMillis();
					
					boolean ret = sm.execute(groupIdx, instanceID, bodyValue, smCtx);
					long eclipseTime = System.currentTimeMillis() - startTimestamp;					
					if (eclipseTime > 100) {
						logger.error("TRACE StateMachine execute costs : {}, smid {}.", eclipseTime, smID);
					}
					
					return ret;
				}
			}
		} catch(Exception e) {
			logger.error("statemachine execute failed : ", e);
		}
		
		logger.error("unknown smID {} instanceID {}.", smID, instanceID);
		return false;
	}
	
	public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
		if (paxosValue == null) {
			logger.error("Value wrong, instanceID {} paxosValue null.", instanceID);
			return true;
		}
		
		if (paxosValue.length < 4) {
			logger.error("Value wrong, instanceID {} size {}.", instanceID, paxosValue.length);
			return true;
		}
		
		try {
			int smID = 0;
			smID = ByteConverter.bytesToIntLittleEndian(paxosValue, 0); 
			
			if (smID == 0) {
				logger.info("Value no need to do sm, just skip, instanceID {}.", instanceID);
			}
			
			int bodyLen = paxosValue.length - 4;
			byte[] bodyValue = new byte[bodyLen];
			System.arraycopy(paxosValue, 4, bodyValue, 0, bodyLen);
			if (smID == Def.BATCH_PROPOSE_SMID) {
				return batchExecuteForCheckpoint(groupIdx, instanceID, bodyValue);
			} else {
				return doExecuteForCheckpoint(groupIdx, instanceID, bodyValue, smID);
			}
		} catch(Exception e) {
			logger.error("SMFac executeForCheckpoint failed.", e);
		}
		
		return false;
	}
	
	public boolean batchExecuteForCheckpoint(int groupIdx, long instanceID, byte[] bodyValue) {	
		try {
			BatchPaxosValue batchPaxosValue = new BatchPaxosValue();
			try {
				batchPaxosValue.parseFromBytes(bodyValue, bodyValue.length);
			} catch(Exception e) {
				logger.error("ParseFromArray failed, value size {}.", bodyValue.length, e);
				return false;
			}
			
			for (int i = 0; i < batchPaxosValue.getBatchSize(); i++) {
				PaxosValue paxosValue = batchPaxosValue.getByIndex(i);
				boolean executeSucc = doExecuteForCheckpoint(groupIdx, instanceID, paxosValue.getValue(), paxosValue.getSmID());
				if (!executeSucc) {
					return false;
				}
			}
			
			return true;
		} catch(Exception e) {
			logger.error("SMFac batchExecuteForCheckpoint failed.", e);
		}
		return false;
	}
	
	public boolean doExecuteForCheckpoint(int groupIdx, long instanceID, byte[] bodyValue, int smID) {
		try {
			if (smID == 0) {
				logger.info("Value no need to do sm, just skip, instanceID {}.", instanceID);
			}
			
			if (this.smList.size() == 0) {
				logger.info("No any sm, need wait sm, instanceID {}.", instanceID);
				return false;
			}
			
			for (StateMachine sm : this.smList) {
				if (sm.getSMID() == smID) {
					return sm.executeForCheckpoint(groupIdx, instanceID, bodyValue);
				}
			}
		} catch(Exception e) {
			logger.error("SMFac doExecuteForCheckpoint failed.", e);
		}		
		
		logger.error("unknown smid {} instanceID {}.", smID, instanceID);
		return false;
	}
	
	public byte[] packPaxosValue (byte[] paxosValue, int len, int smID) {
		byte[] smIDbuf = ByteConverter.intToBytesLittleEndian(smID);
		
		int newLen = len + smIDbuf.length;
		byte[] newBuf = new byte[newLen];
		System.arraycopy(smIDbuf, 0, newBuf, 0, smIDbuf.length);
		System.arraycopy(paxosValue, 0, newBuf, smIDbuf.length, len);
		return newBuf;
	}
	
	public void unPackPaxosValue(byte[] stateBuf, PaxosValue paxosValue) {
		if (stateBuf == null || stateBuf.length < 4) {
			logger.error("unPackPaxosValue, stateBuf null, group {}.", this.myGroupIdx);
		}
		
		byte[] smIDbuf = new byte[4]; //SMID
		System.arraycopy(stateBuf, 0, smIDbuf, 0, 4);
		int smID = ByteConverter.bytesToIntLittleEndian(smIDbuf);
		
		byte[] value = new byte[stateBuf.length - 4];
		System.arraycopy(stateBuf, 4, value, 0, stateBuf.length - 4);
		
		paxosValue.setValue(value);
		paxosValue.setSmID(smID);
	}
	
	public void addSM(StateMachine sm) {
		for (StateMachine smt : this.smList) {
			if (smt.getSMID() == sm.getSMID()) {
				return;
			}
		}
		
		this.smList.add(sm);
	}
	
	public long getCheckpointInstanceID(int groupIdx) {
		long cpInstanceID = -1;
		long cpInstanceidInsize = -1;
		boolean haveUseSM = false;
		
		for (StateMachine sm : this.smList) {
			long checkpointInstanceID = sm.getCheckpointInstanceID(groupIdx);
			if (sm.getSMID() == Def.SYSTEM_V_SMID || sm.getSMID() == Def.MASTER_V_SMID) {
				if (checkpointInstanceID == -1) {
					continue;
				}
				
				if (checkpointInstanceID > cpInstanceidInsize || cpInstanceidInsize == -1) {
					cpInstanceidInsize = checkpointInstanceID;
				}
				continue;
			}
			
			haveUseSM = true;
			
			if (checkpointInstanceID == -1) {
				continue;
			}
			
			if (checkpointInstanceID > cpInstanceID || cpInstanceID == -1) {
				cpInstanceID = checkpointInstanceID;
			}
		}
		
		return haveUseSM ? cpInstanceID : cpInstanceidInsize;
	}

	public List<StateMachine> getSmList() {
		return smList;
	}

	public void setSmList(List<StateMachine> smList) {
		this.smList = smList;
	}
	
	public void beforePropose(int groupIdx, CommitCtx commitCtx) {
		byte[] value = commitCtx.getCommitValue();
		int iSMID = ByteConverter.bytesToIntLittleEndian(value);
		if (iSMID == 0) {
			return;
		}

		if (iSMID == Def.BATCH_PROPOSE_SMID) {
			byte[] bytes = beforeBatchPropose(groupIdx, value);
			commitCtx.setCommitValue(bytes);
		} else {
			JavaOriTypeWrapper<Boolean> changed = new JavaOriTypeWrapper<>();
			changed.setValue(false);
			byte[] bytes = beforeProposeCall(groupIdx, iSMID, value, changed);
			if(changed.getValue()){
				commitCtx.setCommitValue(bytes);
			}
		}
	}
	
	public byte[] beforeBatchPropose(int groupIdx, byte[] value) {
		return value;
	}
	
	public byte[] beforeProposeCall(int groupIdx, int smID, byte[] value, JavaOriTypeWrapper<Boolean> isChange) {
		if (smID == 0) {
			return value;
		}

		if (smList.size() == 0) {
			return value;
		}

		for (StateMachine stateMachine : smList) {
			if (stateMachine.getSMID() == smID) {
				if (stateMachine.needCallBeforePropose()) {
					byte[] body = new byte[value.length - 4];
					System.arraycopy(value, 4, body, 0, body.length);
					isChange.setValue(true);
					byte[] bytes = stateMachine.beforePropose(groupIdx, body);
					return packPaxosValue(bytes, bytes.length, smID);
				}
			}
		}
		return value;
	}
	
	public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID) {
		for (StateMachine sm : this.smList) {
			if (sm.getSMID() == Def.SYSTEM_V_SMID || sm.getSMID() == Def.MASTER_V_SMID) {
				continue;
			}
			
			sm.fixCheckpointByMinChosenInstanceId(minChosenInstanceID);
		}
	}

	public int getMyGroupIdx() {
		return myGroupIdx;
	}
}
