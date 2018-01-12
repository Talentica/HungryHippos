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

import java.sql.Date;

/**
 * Created by rajkishoreh on 12/10/17.
 */
public class MutableDate implements DataTypes{

    private long longValue;

    public MutableDate() {}

    @Override
    public int getLength() {
        return Long.BYTES;
    }


    @Override
    public void reset() {

    }

    @Override
    public String toString() {
        return longValue+"";
    }

    @Override
    public MutableDate clone() {
        MutableDate mutableDate = new MutableDate();
        mutableDate.longValue = this.longValue;
        return mutableDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || !(o instanceof MutableDate)) {
            return false;
        }
        MutableDate that = (MutableDate) o;
        return this.longValue == that.longValue;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(longValue);
    }

    @Override
    public int compareTo(DataTypes dataType) {
        MutableDate otherMutableDate = null;
        if (dataType instanceof MutableDate) {
            otherMutableDate = (MutableDate) dataType;
        } else {
            return -1;
        }
        return Long.compare(this.longValue,otherMutableDate.longValue);
    }

    @Override
    public DataTypes addValue(String value) {
        longValue = Date.valueOf(value).getTime();
        return this;
    }

    public long getLongValue() {
        return longValue;
    }

}
