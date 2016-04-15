package com.talentica.hungryHippos.sharding;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by AmitG on 3/30/2016.
 */
public class NodeCombination implements Serializable{

    private Set<Node> nodes;

    private int index;

    public Set<Node> getNodes() {
        return nodes;
    }

    public void setNodes(Set<Node> nodes) {
        this.nodes = nodes;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("NodeCombination{ index : " + index + ", nodes : [" );
        if (nodes != null && !nodes.isEmpty()) {
            int i = 0;
            for(Node node: nodes){
                if(i != 0){
                    stringBuilder.append(",");
                }
                stringBuilder.append(node.toString());
                i++;
            }
            stringBuilder.append("]}");
            return stringBuilder.toString();
        }
        return super.toString();
    }
}
