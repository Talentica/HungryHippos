package com.talentica.hungryHippos.sharding;

import java.io.Serializable;

/**
 * Created by AmitG on 4/4/2016.
 */
public class NodeCombinationArray implements Serializable{

    private NodeCombination[] nodeCombinations;

    public NodeCombination[] getNodeCombinations() {
        return nodeCombinations;
    }

    public void setNodeCombinations(NodeCombination[] nodeCombinations) {
        this.nodeCombinations = nodeCombinations;
    }

    @Override
    public String toString() {
        if (nodeCombinations != null && nodeCombinations.length > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("NodeCombinationArray[");
            int j = 0;
            for(int i = 0 ; i < nodeCombinations.length ; i++){
                NodeCombination nodeCombination = nodeCombinations[i];
                if(nodeCombination != null){
                    if(j != 0){
                        stringBuilder.append(",");
                    }
                    stringBuilder.append(nodeCombinations[i].toString());
                    j++;
                }
            }
            stringBuilder.append("]");
            return stringBuilder.toString();
        }
        return super.toString();
    }
}
