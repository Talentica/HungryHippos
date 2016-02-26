/**
 * 
 */
package com.talentica.hungryHippos.utility;


/**
 * @author PooshanS
 *
 */
public class TrieNode {
	 	TrieNode[] children = new TrieNode[(1<<8) - 1];
	    boolean isLeaf;
	    String word;
	    public TrieNode() {}
}
