package org.ex.spring.batch.repository;

import java.math.BigInteger;

import org.ex.spring.batch.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<User, BigInteger>{

}
