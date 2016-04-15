package com.talentica.hungryHippos.tester.web.job.output.data;

import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.talentica.hungryHippos.tester.web.job.data.Job;

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
	@OneToOne
	@JoinColumn(name = "job_id")
	@JsonIgnore
	private Job job;

	@Getter
	@Setter
	@Column(name = "data_location")
	private String dataLocation;

	@Getter
	@Setter
	@Column(name = "data_size_in_kbs")
	private BigDecimal dataSize;

}
