/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.sharding;

import java.util.Comparator;

/**
 *{@code NodeRemainingCapacityComparator}  used for compairing remaining capacity of nodes.
 * @author debasishc 
 * @since 14/8/15.
 */
public class NodeRemainingCapacityComparator implements Comparator<Node> {
    @Override
    public int compare(Node o1, Node o2) {
        if(o1.getRemainingCapacity() > o2.getRemainingCapacity()){
            return -1;
        }else if(o1.getRemainingCapacity() == o2.getRemainingCapacity()){
            return 0;
        }else{
            return 1;
        }
    }
}
