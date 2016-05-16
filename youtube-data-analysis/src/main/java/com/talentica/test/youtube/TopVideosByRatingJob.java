package com.talentica.test.youtube;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

public class TopVideosByRatingJob implements Job {

	protected int[] dimensions;
	protected int primaryDimension;
	private int valueIndex;

	@Override
	public Work createNewWork() {
		return new TopVideosWork(dimensions, primaryDimension, valueIndex);
	}

	@Override
	public int[] getDimensions() {
		return dimensions;
	}

	@Override
	public int getIndex() {
		return valueIndex;
	}

	@Override
	public long getMemoryFootprint(long arg0) {
		return 0;
	}

	@Override
	public int getPrimaryDimension() {
		return primaryDimension;
	}

}
