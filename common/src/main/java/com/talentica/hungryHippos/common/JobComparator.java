/**
 * 
 */
package com.talentica.hungryHippos.common;

import java.util.Comparator;

import com.talentica.hungryHippos.client.job.Job;

/**
 * Job Size comparator is to compare the job based on the row counts i.e data size.
 * 
 * @author PooshanS
 *
 */
public class JobComparator implements Comparator<Job>{
	@Override
	public int compare(Job o1, Job o2) {
		return (int) (o1.getJobId() - o2.getJobId());
	}

}
