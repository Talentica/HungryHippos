package com.talentica.hungryHippos.tester.web.job.status.data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import com.fasterxml.jackson.annotation.JsonValue;

@Entity
public class Process {

	public enum PROCESS_NAME {

		FILE_DOWNLOAD("File Download"), SAMPLING("Sampling"), SHARDING("Sharding"), DATA_PUBLISHING(
				"Data Publishing"), JOB_EXECUTION("Job Execution"), OUTPUT_TRANSFER("Transferring output");

		private String name;

		@JsonValue
		public String getName() {
			return name;
		}

		private PROCESS_NAME(String name) {
			this.name = name;
		}
	}

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "process_id")
	private Integer processId;

	@Enumerated(EnumType.STRING)
	@Column(name = "name")
	private PROCESS_NAME name;

	@Column(name = "description")
	private String description;

	public Integer getProcessId() {
		return processId;
	}

	public void setProcessId(Integer processId) {
		this.processId = processId;
	}

	public PROCESS_NAME getName() {
		return name;
	}

	public void setName(PROCESS_NAME name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof Process && processId != null) {
			Process otherprocess = (Process) obj;
			return processId.equals(otherprocess.processId);
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (processId != null) {
			return processId.hashCode();
		}
		return 0;
	}

}
