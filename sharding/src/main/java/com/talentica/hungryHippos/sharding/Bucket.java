package com.talentica.hungryHippos.sharding;

import java.io.Serializable;
import java.util.Collection;

public class Bucket<T> implements Comparable<Bucket<T>>,Serializable{

	private static final long serialVersionUID = 4664630705942019835L;

	private Integer id;

	private long size = 0;

	private long numberOfObjects = 0;

	public Integer getId() {
		return id;
	}

	public Bucket(int id, long size) {
		this.id = id;
		this.size = size;
	}

	public void add(T t) {
		numberOfObjects++;
	}

	public void addAll(Collection<T> t) {
		numberOfObjects = numberOfObjects + t.size();
	}

	public void remove(T t) {
		numberOfObjects--;
	}

	public void removeAll(Collection<T> t) {
		numberOfObjects = numberOfObjects - t.size();
	}

	public void clear() {
		numberOfObjects = 0;
	}

	public long getSize() {
		return size;
	}

	public Long getFilledSize() {
		return numberOfObjects;
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

	@Override
	public String toString() {
		if (id != null) {
			return "Bucket{" + id + "}";
		}
		return super.toString();
	}

}
