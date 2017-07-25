package com.asiainfo.sdtp;

import java.util.HashMap;

public class CountEntity {
	private long sendNum = 0L;
	private long receiveTotalNum = 0L;
	private long receiveXdrNum = 0L;
	private long processPreNum = 0L;
	private long processAfterNum = 0L;
	private HashMap<String,Long> sendTopicNum=new HashMap<String,Long>();
	public CountEntity(long receiveTotalNum,long receiveXdrNum,long sendNum,HashMap<String,Long> sendTopicNum){
		
		this.receiveTotalNum=receiveTotalNum;
		this.receiveXdrNum=receiveXdrNum;
		this.sendNum=sendNum;
		this.sendTopicNum=sendTopicNum;
	}
    public CountEntity(long receiveTotalNum,long receiveXdrNum,long sendNum,HashMap<String,Long> sendTopicNum,long processPreNum,long processAfterNum){
		
		this.receiveTotalNum=receiveTotalNum;
		this.receiveXdrNum=receiveXdrNum;
		this.sendNum=sendNum;
		this.sendTopicNum=sendTopicNum;
		this.processPreNum=processPreNum;
		this.processAfterNum=processAfterNum;
	}
	public long getProcessPreNum() {
		return processPreNum;
	}
	public void setProcessPreNum(long processPreNum) {
		this.processPreNum = processPreNum;
	}
	public long getProcessAfterNum() {
		return processAfterNum;
	}
	public void setProcessAfterNum(long processAfterNum) {
		this.processAfterNum = processAfterNum;
	}
	public long getSendNum() {
    	return sendNum;
    }
	public void setSendNum(long sendNum) {
    	this.sendNum = sendNum;
    }
	public long getReceiveTotalNum() {
    	return receiveTotalNum;
    }
	public void setReceiveTotalNum(long receiveTotalNum) {
    	this.receiveTotalNum = receiveTotalNum;
    }
	public long getReceiveXdrNum() {
    	return receiveXdrNum;
    }
	public void setReceiveXdrNum(long receiveXdrNum) {
    	this.receiveXdrNum = receiveXdrNum;
    }
	public HashMap<String, Long> getSendTopicNum() {
    	return sendTopicNum;
    }
	public void setSendTopicNum(HashMap<String, Long> sendTopicNum) {
    	this.sendTopicNum = sendTopicNum;
    }

}
