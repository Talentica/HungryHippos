package com.talentica.hungryHippos.utility;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author PooshanS
 *
 */
public class TrieTest {
	
	private String[] stmnt;
	private Trie trie;
	private static final String expected1 = "WORLD";
	private static final String expected2 = "4";
	
	@Before
	public void setUp(){
		stmnt = "HELLO HE WORLD 12 445 ALLOW".trim().split(" ");
		trie = new Trie();
		for (int i = 0; i < stmnt.length; i++) {
			trie.insert(stmnt[i]);
		}
	}
	
	@Test
	public void searchStringTest(){
		Assert.assertTrue(trie.search(expected1));
	}
	
	@Test
	public void startWithStringTest(){
		Assert.assertTrue(trie.startsWith(expected2));
	}
	
	@Test
	public void getWord(){
		List<String> words = new ArrayList<>();
		trie.getAllWords(trie.getRootNode(),words);
		System.out.println(words.toString());
	}

}
