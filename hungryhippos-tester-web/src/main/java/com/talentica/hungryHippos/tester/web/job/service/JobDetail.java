package com.talentica.hungryHippos.tester.web.job.service;

import org.joda.time.Interval;

import com.talentica.hungryHippos.tester.web.job.data.Job;
import com.talentica.hungryHippos.tester.web.job.data.JobInput;
import com.talentica.hungryHippos.tester.web.job.output.data.JobOutput;

import lombok.Getter;
import lombok.Setter;

public class JobDetail {

	@Getter
	private Job job;

	@Getter
	@Setter
	private JobOutput jobOutput;

	@Getter
	private Long executionTimeInSeconds;

	public JobDetail() {
	}

	public JobDetail(Job job, JobInput jobInput) {
		setJob(job);
	}

	public void setExecutionTimeInSeconds() {
		org.joda.time.Duration duration = getExecutionDuration();
		if (duration != null) {
			executionTimeInSeconds = duration.getStandardSeconds();
		}
	}

	private org.joda.time.Duration getExecutionDuration() {
		org.joda.time.Duration duration = null;
		if (job != null && job.getDateTimeFinished() != null && job.getDateTimeSubmitted() != null) {
			Interval executionInterval = new Interval(job.getDateTimeSubmitted().getTime(),
					job.getDateTimeFinished().getTime());
			duration = executionInterval.toDuration();
		}
		return duration;
	}

	public void setJob(Job job) {
		this.job = job;
		setExecutionTimeInSeconds();
	}

}
