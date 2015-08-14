package com.talentica.hungryHippos;

import java.util.Comparator;

/**
 * Created by debasishc on 14/8/15.
 */
public class NodeRemainingCapacityComparator implements Comparator<Node> {
    @Override
    public int compare(Node o1, Node o2) {
        if(o1.getRemainingCapacity() > o2.getRemainingCapacity()){
            return 1;
        }else if(o1.getRemainingCapacity() == o2.getRemainingCapacity()){
            return 0;
        }else{
            return -1;
        }
    }
}
