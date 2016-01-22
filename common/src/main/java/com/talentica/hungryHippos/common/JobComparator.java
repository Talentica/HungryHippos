/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.util.Comparator;

/**
 * Job Size comparator is to compare the job based on the row counts i.e data size.
 * 
 * @author PooshanS
 *
 */
public class JobComparator implements Comparator<JobEntity>{
	@Override
	public int compare(JobEntity o1, JobEntity o2) {
		return (int) (o1.getJob().getJobId() - o2.getJob().getJobId());
	}

}
