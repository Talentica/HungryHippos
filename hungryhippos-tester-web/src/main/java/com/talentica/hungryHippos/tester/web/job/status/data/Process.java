package com.talentica.hungryHippos.tester.web.job.status.data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import com.fasterxml.jackson.annotation.JsonValue;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "processId")
public class Process {
	
	public enum PROCESS_NAME{
		
		FILE_DOWNLOAD("File Download"),
		SAMPLING("Sampling"),
		SHARDING("Sharding"),
		DATA_PUBLISHING("Data Publishing"),
		JOB_EXECUTION("Job Execution"),
		OUTPUT_TRANSFER("Transferring output");

		private String name;
		
		@JsonValue
		public String getName() {
			return name;
		}
		
		private PROCESS_NAME(String name){
			this.name=name;
		}
	}

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "process_id")
	private Integer processId;

	@Getter
	@Setter
	@Enumerated(EnumType.STRING)
	@Column(name = "name")
	private PROCESS_NAME name;

	@Getter
	@Setter
	@Column(name = "description")
	private String description;

}
