package com.talentica.hungryHippos.tester.web.job.output.data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "jobOutputId")
public class JobOutput {

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "job_output_id")
	private Integer jobOutputId;

	@Getter
	@Setter
	@Column(name = "job_id")
	private Integer jobId;

	@Getter
	@Setter
	@Column(name = "data_location")
	private String dataLocation;

	@Getter
	@Setter
	@Column(name = "data_size")
	private Integer dataSize;

}
