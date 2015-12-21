package com.talentica.hungryHippos.accumulator;

import com.talentica.hungryHippos.storage.RowProcessor;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by debasishc on 9/9/15.
 */
public class TestRowProcessor implements RowProcessor {
    FieldTypeArrayDataDescription dataDescription;
    DynamicMarshal dynamicMarshal;


    public TestRowProcessor(FieldTypeArrayDataDescription dataDescription,
                            DynamicMarshal dynamicMarshal) {
        this.dataDescription = dataDescription;
        this.dynamicMarshal = dynamicMarshal;
    }

    @Override
    public void processRow(ByteBuffer row) {
        System.out.println(dynamicMarshal.readValue(0, row));
    }

    @Override
    public void finishUp() {

    }

	@Override
	public void rowCount(ByteBuffer row) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void finishRowCount() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<Integer,Integer> getTotalRowCountByJobId() {
		// TODO Auto-generated method stub
		return null;
	}
}
