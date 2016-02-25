package com.talentica.hungryHippos.test.sum;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.Work;
import com.talentica.hungryHippos.client.job.Job;

/**
 * Created by debasishc on 9/9/15.
 */
public class SumJob implements Job,Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -4111336293020419218L;
	protected int [] dimensions;
    protected int primaryDimension;
    protected int valueIndex;
    public SumJob(){}

    public SumJob(int[] dimensions, int primaryDimension, int valueIndex) {
        this.dimensions = dimensions;
        this.primaryDimension = primaryDimension;
        this.valueIndex = valueIndex;
    }


    @Override
    public Work createNewWork() {
        return new SumWork(dimensions,primaryDimension,valueIndex);
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
	public long getMemoryFootprint(int rowCount) {
		return 8;
	}

	@Override
	public String toString() {
		if (dimensions != null) {
			return "\nSumJob{{primary dim:" + primaryDimension + ",dimensions" + Arrays.toString(dimensions)
					+ ", valueIndex:" + valueIndex + "}}";
		}
		return super.toString();
	}

	@Override
	public int getIndex() {
		return valueIndex;
	}

}
