package com.talentica.test.youtube;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.client.job.JobMatrix;

public class YoutubeVideosMatrixImpl implements JobMatrix,Serializable {

	private static final long serialVersionUID = 7091647562756500530L;

	@Override
	public List<Job> getListOfJobsToExecute() {
		List<Job> jobs = new ArrayList<Job>();
		jobs.add(new TopVideosByRatingJob());
		return jobs;
	}

	public static void main(String[] args) {
		System.out.println(new YoutubeVideosMatrixImpl().getListOfJobsToExecute());
	}

}
