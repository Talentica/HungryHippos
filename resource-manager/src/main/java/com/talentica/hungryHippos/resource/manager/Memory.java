package com.talentica.hungryHippos.resource.manager;

import java.math.BigDecimal;

public class Memory implements Resource {

	private BigDecimal bytes;

	public Memory(long value) {
		bytes = BigDecimal.valueOf(value);
	}

	@Override
	public BigDecimal getValue() {
		return bytes;
	}

	@Override
	public int compareTo(Resource otherResource) {
		return getValue().compareTo(otherResource.getValue());
	}

	@Override
	public String toString() {
		return "Memory{" + bytes + "}";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj != null && obj instanceof Memory) {
			Memory other = (Memory) obj;
			if (other.getValue() != null && getValue() != null) {
				return other.getValue().equals(getValue());
			}
		}
		return false;
	}

}
