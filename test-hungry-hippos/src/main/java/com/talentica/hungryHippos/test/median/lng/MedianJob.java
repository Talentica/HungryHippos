package com.talentica.hungryHippos.test.median.lng;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

/**
 * Created by debasishc on 9/9/15.
 */
public class MedianJob implements Job,Serializable{

	private static final long serialVersionUID = -4111336293020419218L;
	protected int [] dimensions;
    protected int primaryDimension;
    protected int valueIndex;
    public MedianJob(){}

    public MedianJob(int[] dimensions, int primaryDimension, int valueIndex) {
        this.dimensions = dimensions;
        this.primaryDimension = primaryDimension;
        this.valueIndex = valueIndex;
    }


    @Override
    public Work createNewWork() {
		return new MedianWork(dimensions, primaryDimension, valueIndex);
    }

    @Override
    public int[] getDimensions() {
        return dimensions;
    }

    @Override
    public int getPrimaryDimension() {
        return primaryDimension;
    }

	@Override
	public long getMemoryFootprint(long rowCount) {
		return 58 * rowCount;
	}

	@Override
	public String toString() {
		if (dimensions != null) {
			return "\nMedianJob{{primary dim:" + primaryDimension + ",dimensions" + Arrays.toString(dimensions)
					+ ", valueIndex:" + valueIndex + "}}";
		}
		return super.toString();
	}

	@Override
	public int getIndex() {
		return valueIndex;
	}

}
