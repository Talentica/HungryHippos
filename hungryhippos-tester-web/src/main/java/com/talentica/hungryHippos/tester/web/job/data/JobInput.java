package com.talentica.hungryHippos.tester.web.job.data;

import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import com.fasterxml.jackson.annotation.JsonIgnore;

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

	@Getter
	@Setter
	@Column(name = "data_type_configuration")
	private String dataTypeConfiguration;

	@Getter
	@Setter
	@Column(name = "sharding_dimensions")
	private String shardingDimensions;

	@Getter
	@Setter
	@Column(name = "job_matrix_class")
	private String jobMatrixClass;

}
