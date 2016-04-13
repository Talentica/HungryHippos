package com.talentica.hungryHippos.tester.web;

import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

@Configuration
@EnableWebSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

	@Autowired(required = false)
	private DataSource dataSource;

	@Override
	protected void configure(HttpSecurity http) throws Exception {
		AuthenticationFailureHandler authenticationFailureHandler = new AuthenticationFailureHandler();
		http.csrf().disable().authorizeRequests().antMatchers("/secure/**").authenticated().and().formLogin()
				.permitAll().defaultSuccessUrl("/secure/welcome.html").failureHandler(authenticationFailureHandler)
				.and().logout().logoutSuccessUrl("/index.html");
	}

	public class AuthenticationFailureHandler extends SimpleUrlAuthenticationFailureHandler {

		public AuthenticationFailureHandler() {
			setDefaultFailureUrl("/index.html");
		}

		@Override
		public void onAuthenticationFailure(javax.servlet.http.HttpServletRequest request,
				javax.servlet.http.HttpServletResponse response,
				org.springframework.security.core.AuthenticationException exception)
						throws java.io.IOException, javax.servlet.ServletException {
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
		};
	}

	@Override
	protected void configure(AuthenticationManagerBuilder auth) throws Exception {
		auth.jdbcAuthentication().dataSource(dataSource)
				.usersByUsernameQuery("select email_address,password,true FROM user where email_address=?")
				.authoritiesByUsernameQuery(
						"select email_address,role from user u,user_role ur,role r where r.role_id=ur.role_id and"
								+ " ur.user_id=u.user_id and u.email_address=?");
	}

}
