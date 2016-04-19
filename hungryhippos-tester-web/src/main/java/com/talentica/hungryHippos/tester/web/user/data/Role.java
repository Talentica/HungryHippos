package com.talentica.hungryHippos.tester.web.user.data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class Role {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "role_id")
	private Integer roleId;

	@Column(name = "role")
	private String role;

	public Integer getRoleId() {
		return roleId;
	}

	public void setRoleId(Integer roleId) {
		this.roleId = roleId;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof Role && roleId != null) {
			Role other = (Role) obj;
			return roleId.equals(other.roleId);
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (roleId != null) {
			return roleId.hashCode();
		}
		return 0;
	}

}