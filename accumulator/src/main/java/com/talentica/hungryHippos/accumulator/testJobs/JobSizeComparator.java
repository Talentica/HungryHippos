/**
 * 
 */
package com.talentica.hungryHippos.accumulator.testJobs;

import java.util.Comparator;

import com.talentica.hungryHippos.accumulator.Job;

/**
 * Job Size comparator is to compare the job based on the row counts i.e data size.
 * 
 * @author PooshanS
 *
 */
public class JobSizeComparator implements Comparator<Job>{
	@Override
	public int compare(Job o1, Job o2) {
		return (int) (o1.getDataSize() - o2.getDataSize());
	}

}
