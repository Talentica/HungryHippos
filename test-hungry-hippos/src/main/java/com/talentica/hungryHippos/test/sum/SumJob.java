package com.talentica.hungryHippos.test.sum;

import java.io.Serializable;

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
    protected int jobId;
    protected long dataSize;
    protected String status;
    public SumJob(){}

    public SumJob(int[] dimensions, int primaryDimension, int valueIndex, int jobId) {
        this.dimensions = dimensions;
        this.primaryDimension = primaryDimension;
        this.valueIndex = valueIndex;
        this.jobId = jobId;
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
	public int getJobId() {
		return jobId;
	}
	@Override
	public void status(String status) {
		this.status = status;
	}
	
	@Override
	public long getMemoryFootprint(int rowCount) {
		return 10*rowCount;
	}

}
