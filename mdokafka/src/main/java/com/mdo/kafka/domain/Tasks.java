package com.mdo.kafka.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

//import org.springframework.data.cassandra.mapping.Column;
//import org.springframework.data.cassandra.mapping.PrimaryKey;
//import org.springframework.data.cassandra.mapping.Table;

//@Table(value = "TASK")
public class Tasks implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	//@PrimaryKey
	//@Column("taskId")
	private int taskId;
	

	private int documentId;

	private String type;
	private String status;
	private boolean isUrgent;
	private String taskItem;
	private String taskItemStatus;
	private String taskItemUser;
	private String taskItemTimeStamp;

	/*
	 * private String type; private boolean taskStatus; private String status;
	 * private boolean urgency; private List<String> taskDetails;
	 */
	public Tasks() {
		

	}
	
	public Tasks(int taskId, int documentId, String type, String status,
			boolean isUrgent, String taskItem, String taskItemStatus,
			String taskItemUser, String taskItemTimeStamp) {
		this.taskId = taskId;
		this.documentId = documentId;
		this.type = type;
		this.status = status;
		this.isUrgent = isUrgent;
		this.taskItem = taskItem;
		this.taskItemStatus = taskItemStatus;
		this.taskItemUser = taskItemUser;
		this.taskItemTimeStamp = taskItemTimeStamp;

	}

	/*
	 * public Task() { super(); this.taskDetails = new ArrayList<String>();
	 * 
	 * }
	 */

	/*
	 * public Task(String type, boolean taskStatus, String status, boolean
	 * urgency) { super(); this.type = type; this.taskStatus = taskStatus;
	 * this.status = status; this.urgency = urgency; this.taskDetails = new
	 * ArrayList<String>(); }
	 */

	/*
	 * public String getType() { return type; }
	 * 
	 * public void setType(String type) { this.type = type; }
	 * 
	 * public boolean isTaskStatus() { return taskStatus; }
	 * 
	 * public void setTaskStatus(boolean taskStatus) { this.taskStatus =
	 * taskStatus; }
	 * 
	 * public String getStatus() { return status; }
	 * 
	 * public void setStatus(String status) { this.status = status; }
	 * 
	 * public boolean isUrgency() { return urgency; }
	 * 
	 * public void setUrgency(boolean urgency) { this.urgency = urgency; }
	 * 
	 * public List<String> getTaskDetails() { return taskDetails; }
	 * 
	 * public void setTaskDetails(List<String> taskDetails) { this.taskDetails =
	 * taskDetails; }
	 */

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public int getDocumentId() {
		return documentId;
	}

	public void setDocumentId(int documentId) {
		this.documentId = documentId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public boolean isUrgent() {
		return isUrgent;
	}

	public void setUrgent(boolean isUrgent) {
		this.isUrgent = isUrgent;
	}

	public String getTaskItem() {
		return taskItem;
	}

	public void setTaskItem(String taskItem) {
		this.taskItem = taskItem;
	}

	public String getTaskItemStatus() {
		return taskItemStatus;
	}

	public void setTaskItemStatus(String taskItemStatus) {
		this.taskItemStatus = taskItemStatus;
	}

	public String getTaskItemUser() {
		return taskItemUser;
	}

	public void setTaskItemUser(String taskItemUser) {
		this.taskItemUser = taskItemUser;
	}

	public String getTaskItemTimeStamp() {
		return taskItemTimeStamp;
	}

	public void setTaskItemTimeStamp(String taskItemTimeStamp) {
		this.taskItemTimeStamp = taskItemTimeStamp;
	}

}
