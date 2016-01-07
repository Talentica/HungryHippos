/**
 * 
 */
package com.talentica.hungryHippos.accumulator.testJobs;

import java.util.Comparator;

import com.talentica.hungryHippos.accumulator.JobEntity;

/**
 * Job Size comparator is to compare the job based on the row counts i.e data size.
 * 
 * @author PooshanS
 *
 */
public class JobComparator implements Comparator<JobEntity>{
	@Override
	public int compare(JobEntity o1, JobEntity o2) {
		return (int) (o1.getRowCount() - o2.getRowCount());
	}

}
