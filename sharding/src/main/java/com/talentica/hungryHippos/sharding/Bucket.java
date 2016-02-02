package com.talentica.hungryHippos.sharding;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Bucket<T> implements Comparable<Bucket<T>>,Serializable{

	private static final long serialVersionUID = 4664630705942019835L;

	private Integer id;

	private long size = 0;

	private List<T> objects = new ArrayList<>();

	public Integer getId() {
		return id;
	}

	public Bucket(int id, long size) {
		this.id = id;
		this.size = size;
	}

	public void add(T t) {
		objects.add(t);
	}

	public void addAll(Collection<T> t) {
		objects.addAll(t);
	}

	public void remove(T t) {
		objects.add(t);
	}

	public void removeAll(Collection<T> t) {
		objects.removeAll(t);
	}

	public void clear() {
		objects.clear();
	}

	public long getSize() {
		return size;
	}

	public Long getFilledSize() {
		if (objects != null) {
			return (long) objects.size();
		}
		return 0l;
	}

	@Override
	public int hashCode() {
		if (id != null) {
			return id.hashCode();
		}
		return super.hashCode();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj != null && obj instanceof Bucket) {
			return ((Bucket) obj).getId().equals(getId());
		}
		return false;
	}

	@Override
	public int compareTo(Bucket<T> otherBucket) {
		if (otherBucket != null) {
			return getFilledSize().compareTo(otherBucket.getFilledSize());
		}
		return 0;
	}

	public Iterator<T> iterator() {
		return objects.iterator();
	}

	@Override
	public String toString() {
		if (id != null) {
			return "Bucket{" + id + "}";
		}
		return super.toString();
	}

}
