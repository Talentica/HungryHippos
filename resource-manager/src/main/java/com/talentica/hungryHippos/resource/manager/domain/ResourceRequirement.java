package com.talentica.hungryHippos.resource.manager.domain;

/**
 * Represents resource requirements to perform any work. e.g. To run a job, you
 * need certain resources - RAM, disk memory etc. It can be passed to
 * {@link ResourceManager} in the form of {@link ResourceRequirement}.
 * 
 * @author nitink
 *
 */
public final class ResourceRequirement {

	private Long diskSize;

	private Long ram;

	public ResourceRequirement(long diskSizeNeeded, long ramNeeded) {
		this.diskSize = diskSizeNeeded;
		this.ram = ramNeeded;
	}

	/**
	 * Returns no. of bytes of free disk size needed.
	 */
	public Long getDiskSize() {
		return diskSize;
	}

	/**
	 * Returns no. of bytes of RAM needed.
	 * 
	 * @return
	 */
	public Long getRam() {
		return ram;
	}

	@Override
	public String toString() {
		return "Memory{RAM:" + ram + " bytes, " + diskSize + " bytes}";
	}

	@Override
	public boolean equals(Object obj) {
		boolean areEqual = false;
		if (this == obj) {
			return true;
		}
		if (obj != null && obj instanceof ResourceRequirement) {
			ResourceRequirement other = (ResourceRequirement) obj;
			if (other.getRam() != null) {
				areEqual = other.getRam().equals(getRam()) && isDiskSizeEqual(other);
			}
		}
		return areEqual;
	}

	private boolean isDiskSizeEqual(ResourceRequirement other) {
		boolean areEqual = false;
		if (other.getDiskSize() != null) {
			areEqual = other.getDiskSize().equals(getDiskSize());
		} else if (getDiskSize() != null) {
			areEqual = getDiskSize().equals(other.getDiskSize());
		}
		return areEqual;
	}

}