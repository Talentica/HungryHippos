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

package com.talentica.hungryHippos.node.joiners.orc;

import com.talentica.hungryHippos.client.domain.DataDescription;
import org.apache.orc.TypeDescription;

/**
 * Created by rajkishoreh on 7/5/18.
 */

public class OrcSchemaCreator {
    public static TypeDescription createPrimitiveSchema(DataDescription dataDescription, String[] names){
        TypeDescription schema = TypeDescription.createStruct();

        for (int i = 0; i < dataDescription.getNumberOfDataFields(); i++) {
            switch (dataDescription.locateField(i).getDataType()){
                case BYTE:
                    schema.addField(names[i],TypeDescription.createByte());
                    break;
                case SHORT:
                    schema.addField(names[i],TypeDescription.createShort());
                    break;
                case INT:
                    schema.addField(names[i],TypeDescription.createInt());
                    break;
                case LONG:
                    schema.addField(names[i],TypeDescription.createLong());
                    break;
                case FLOAT:
                    schema.addField(names[i],TypeDescription.createFloat());
                    break;
                case DOUBLE:
                    schema.addField(names[i],TypeDescription.createDouble());
                    break;
                case STRING:
                    schema.addField(names[i],TypeDescription.createString());
                    break;
                case TIMESTAMP:
                    schema.addField(names[i],TypeDescription.createTimestamp());
                    break;
                case DATE:
                    schema.addField(names[i],TypeDescription.createDate());
                    break;
            }
        }
        return schema;

    }
}
