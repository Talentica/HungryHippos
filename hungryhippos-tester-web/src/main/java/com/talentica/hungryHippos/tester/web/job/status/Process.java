package com.talentica.hungryHippos.tester.web.job.status;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "processId")
public class Process {

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "process_id")
	private Integer processId;

	@Getter
	@Setter
	@Column(name = "name")
	private String name;

	@Getter
	@Setter
	@Column(name = "description")
	private String description;

}
