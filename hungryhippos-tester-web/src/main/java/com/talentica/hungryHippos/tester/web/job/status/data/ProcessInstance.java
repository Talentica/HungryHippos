package com.talentica.hungryHippos.tester.web.job.status.data;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "processInstanceId")
public class ProcessInstance {

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "process_instance_id")
	private Integer processInstanceId;

	@Getter
	@Setter
	@OneToOne
	@JoinColumn(name = "process_id")
	private Process process;

	@Getter
	@Setter
	@Column(name = "job_id")
	private Integer jobId;

	@Getter
	@Setter
	@OneToMany(mappedBy = "processInstanceId", fetch = FetchType.EAGER)
	@PrimaryKeyJoinColumn
	private List<ProcessInstanceDetail> processInstanceDetails;

}
