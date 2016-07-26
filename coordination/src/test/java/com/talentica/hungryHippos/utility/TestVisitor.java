package com.talentica.hungryHippos.utility;


import java.util.Set;

import org.junit.Assert;

import com.google.common.collect.Sets;
import com.talentica.hungryHippos.coordination.utility.ObjectGraph;

/**
* @author bramp
*/
class TestVisitor implements ObjectGraph.Visitor {
	public final Set<Object> found = Sets.newIdentityHashSet();

	@Override
	public boolean visit(Object object, Class clazz) {

		System.out.println(clazz.toString() + " " + object.toString());

		Assert.assertNotNull(object);
		Assert.assertNotNull(clazz);

		// Check we haven't double visited this node
		//Assert.assertThat(found).usingElementComparator(Ordering.arbitrary()).doesNotContain(object);

		found.add(object);
		return false;
	}
}
