package com.talentica.test.youtube;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

public class TopVideosByRatingJob implements Job, Serializable {

	private static final long serialVersionUID = -8299385914889558642L;

	protected int[] dimensions = new int[] { 3 };

	protected int primaryDimension = 3;

	private int valueIndex = 6;

	@Override
	public Work createNewWork() {
		return new TopVideosWork(dimensions, primaryDimension);
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

	@Override
	public String toString() {
		if (dimensions != null) {
			return "\nTopVideosByRatingJob{{primary dim:" + primaryDimension + ",dimensions"
					+ Arrays.toString(dimensions) + ", valueIndex:" + valueIndex + "}}";
		}
		return super.toString();
	}

}
