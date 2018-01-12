/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.client.domain;

import java.sql.Timestamp;

/**
 * Created by rajkishoreh on 12/10/17.
 */

public class MutableTimeStamp implements DataTypes{

    private long longValue;

    public MutableTimeStamp() {

    }

    @Override
    public int getLength() {
        return Long.BYTES;
    }

    @Override
    public String toString() {
        return longValue+"";
    }

    @Override
    public void reset() {

    }

    @Override
    public MutableTimeStamp clone() {
        MutableTimeStamp mutableTimeStamp = new MutableTimeStamp();
        mutableTimeStamp.longValue = this.longValue;
        return mutableTimeStamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || !(o instanceof MutableTimeStamp)) {
            return false;
        }
        MutableTimeStamp that = (MutableTimeStamp) o;
        return this.longValue==that.longValue;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(longValue);
    }

    @Override
    public int compareTo(DataTypes dataType) {
        MutableTimeStamp mutableTimeStamp = null;
        if (dataType instanceof MutableTimeStamp) {
            mutableTimeStamp = (MutableTimeStamp) dataType;
        } else {
            return -1;
        }
        return Long.compare(this.longValue,mutableTimeStamp.longValue);
    }

    @Override
    public DataTypes addValue(String value) {
        longValue = Timestamp.valueOf(value).getTime();
        return this;
    }

    public long getLongValue() {
        return longValue;
    }
}
