package com.talentica.hungryHippos.tester.web;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@Configuration
@EnableWebSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

	@Autowired(required = false)
	private DataSource dataSource;

	@Override
	protected void configure(HttpSecurity http) throws Exception {
		http.csrf().disable().authorizeRequests().antMatchers("/secure/**").authenticated().and().formLogin()
				.permitAll();
		// .failureUrl("/index.html?error=true").defaultSuccessUrl("/secure/welcome.html")
		// .loginPage("/index.html").and().logout().logoutSuccessUrl("/index.html");
	}

	@Override
	protected void configure(AuthenticationManagerBuilder auth) throws Exception {
		auth.jdbcAuthentication().dataSource(dataSource)
				.usersByUsernameQuery("SELECT email_address,password,true FROM user where email_address=?")
				.authoritiesByUsernameQuery(
						"select email_address,role from user u,user_role ur,role r where r.role_id=ur.role_id and"
								+ " ur.user_id=u.user_id and u.email_address=?");
	}

}
