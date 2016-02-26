/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.util.List;



/**
 * @author PooshanS
 *
 */
public class Trie {
	
	 private TrieNode root;
	 
	    public Trie() {
	        root = new TrieNode();
	    }
	    
	    public TrieNode getRootNode(){
	    	return root;
	    }
	 
	    public void insert(String word) {
	    	TrieNode[] children = root.children;
	 
	        for(int i=0; i<word.length(); i++){
	            char c = word.charAt(i);
	            int asciiValue = c;
	            TrieNode t;
	            if(children[asciiValue] != null){
	                    t = children[asciiValue];
	            }else{
	                t = new TrieNode();
	                children[asciiValue] = t;
	            }
	 
	            children = t.children;
	 
	            if(i==word.length()-1)
	                t.isLeaf = true;
	            	t.word = word;
	        }
	    }
	 
	    public boolean search(String word) {
	        TrieNode t = searchNode(word);
	        if(t != null && t.isLeaf) 
	            return true;
	        else
	            return false;
	    }
	 
	    public boolean startsWith(String prefix) {
	        if(searchNode(prefix) == null) 
	            return false;
	        else
	            return true;
	    }
	    
	    public TrieNode searchNode(String str){
	    	TrieNode[] children = root.children; 
	        TrieNode t = null;
	        for(int i=0; i<str.length(); i++){
	            char c = str.charAt(i);
	            int ascii = c;
	            if(children[ascii] != null){
	                t = children[ascii];
	                children = t.children;
	            }else{
	                return null;
	            }
	        }
	 
	        return t;
	    }
	    
	public void getAllWords(TrieNode root,List<String> words) {
		TrieNode[] children = root.children;
		for (int ascii = 0; ascii < children.length; ascii++) {
			if (children[ascii] == null)
				continue;
			else if (children[ascii].isLeaf) {
				words.add(children[ascii].word);
			}
			getAllWords(children[ascii],words);
			
		}
	}
}
