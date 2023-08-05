package org.ex.spring.batch.config;

import org.ex.spring.batch.entity.User;
import org.ex.spring.batch.repository.UserRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
/**
 * @author MILTON
 */
@Component
public class UserItemWriter implements ItemWriter<User> {

	@Autowired
	private UserRepository userRepository;


	@Override
	public void write(Chunk<? extends User> chunk) throws Exception {
		userRepository.saveAll(chunk);
	}

}
