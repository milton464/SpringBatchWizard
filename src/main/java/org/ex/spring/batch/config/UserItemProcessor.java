package org.ex.spring.batch.config;

import org.ex.spring.batch.entity.User;
import org.springframework.batch.item.ItemProcessor;

/**
 * @author MILTON
 */
public class UserItemProcessor implements ItemProcessor<User, User>{

	@Override
	public User process(User user) throws Exception {
		return user;
	}

}
