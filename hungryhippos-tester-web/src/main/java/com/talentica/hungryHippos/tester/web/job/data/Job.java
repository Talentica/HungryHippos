package com.talentica.hungryHippos.tester.web.job.data;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;

import com.talentica.hungryHippos.tester.web.job.STATUS;
import com.talentica.hungryHippos.tester.web.user.data.User;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "jobId")
public class Job {

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "job_id")
	private Integer jobId;

	@Getter
	@Setter
	@Column(name = "job_uuid")
	private String uuid;

	@Getter
	@Setter
	@Enumerated(EnumType.STRING)
	@Column(name = "status")
	private STATUS status;

	@Getter
	@Setter
	@Column(name = "date_time_submitted")
	private Date dateTimeSubmitted;

	@Getter
	@Setter
	@Column(name = "date_time_finished")
	private Date dateTimeFinished;

	@Getter
	@Setter
	@OneToOne(fetch = FetchType.EAGER)
	@PrimaryKeyJoinColumn
	private User user;

}