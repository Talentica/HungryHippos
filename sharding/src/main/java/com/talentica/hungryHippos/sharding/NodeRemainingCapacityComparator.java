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
