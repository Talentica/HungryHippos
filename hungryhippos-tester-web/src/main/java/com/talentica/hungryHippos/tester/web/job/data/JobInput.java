package com.talentica.hungryHippos.tester.web.job.data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "jobInputId")
public class JobInput {

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "job_input_id")
	private Integer jobInputId;

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

	@Getter
	@Setter
	@Column(name = "data_type_configuration")
	private String dataTypeConfiguration;

	@Getter
	@Setter
	@Column(name = "sharding_dimensions")
	private String shardingDimensions;

}
