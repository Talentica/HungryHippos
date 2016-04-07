package com.talentica.hungryHippos.tester.web.user.data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Entity
@EqualsAndHashCode(of = "userId")
public class User {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "user_id")
	@Getter
	@Setter
	private Integer userId;

	@Getter
	@Setter
	@Column(name = "first_name")
	private String firstName;

	@Getter
	@Setter
	@Column(name = "last_name")
	private String lastName;

	@Getter
	@Setter
	@Column(name = "email_address")
	private String emailAddress;

	@Getter
	@Setter
	@Column(name = "password")
	private String password;

}