package org.ex.spring.batch.config;

import java.util.Random;

import org.ex.spring.batch.entity.User;
import org.ex.spring.batch.partition.ColumnRangePartition;
import org.ex.spring.batch.repository.UserRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.AllArgsConstructor;

/**
 * @author MILTON
 */
@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {

	private JobRepository jobRepository;

	private PlatformTransactionManager transactionManager;

	@Autowired
	private UserRepository userRepository;
	
	private UserItemWriter userItemWriter;

	@Bean
	public FlatFileItemReader<User> reader() {
		FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
		flatFileItemReader.setResource(new FileSystemResource("src/main/resources/MOCK_DATA1.csv"));
		flatFileItemReader.setName("csvReader");
		flatFileItemReader.setLinesToSkip(1);
		flatFileItemReader.setLineMapper(lineMapper());

		return flatFileItemReader;
	}

	private LineMapper<User> lineMapper() {
		System.out.println("lineMapper");
		DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("name", "email", "gender");

		BeanWrapperFieldSetMapper<User> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<User>();
		beanWrapperFieldSetMapper.setTargetType(User.class);

		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(beanWrapperFieldSetMapper);

		return lineMapper;
	}

	@SuppressWarnings("rawtypes")
	@Bean
	public ItemProcessor processor() {
		System.out.println("processor.................");
		return new UserItemProcessor();
	}

//	@Bean
//	public RepositoryItemWriter<User> writer() {
//		System.out.println("writer.................");
//		RepositoryItemWriter<User> repositoryItemWriter = new RepositoryItemWriter<>();
//		repositoryItemWriter.setRepository(userRepository);
//		repositoryItemWriter.setMethodName("save");
//		return repositoryItemWriter;
//	}

	@Bean
	public ColumnRangePartition partitioner() {
		 return new ColumnRangePartition();
	}
	
	@Bean
	public PartitionHandler partitionHandler() {
		TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
		taskExecutorPartitionHandler.setGridSize(4);
		taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
		taskExecutorPartitionHandler.setStep(slaveStep());
		return taskExecutorPartitionHandler;
	}
	
	
	@SuppressWarnings("rawtypes")
	@Bean
	public ItemProcessor itemProcessor(){
		System.out.println("itemProcessor.................");
		
		return new ItemProcessor<User, User>() {
			@Override
			public User process(User item) throws Exception {
				Thread.sleep(new Random().nextInt(10));
				return User.builder().id(item.getId()).name(item.getName())
						.email(item.getEmail())
						.gender(item.getGender())
						.build();
			}
		};
	}
	
	@SuppressWarnings({"unchecked"})
	@Bean
	public Step slaveStep() {
		System.out.println("slaveStep....................");
	    return new StepBuilder("slaveStepp", jobRepository)
	    	.chunk(200, transactionManager)
	    	.reader(reader())
	        .processor(processor())
	        .writer(userItemWriter)
	        .build();
	}
	
	@Bean
	public Step masterStep() {
		return new StepBuilder("masterStep", jobRepository)
				.partitioner(slaveStep().getName(), partitioner())
				.partitionHandler(partitionHandler()).build();
	}

	@Bean
	public Job runJob() {
		System.out.println("runJob....................");
		return new JobBuilder("user", jobRepository).incrementer(new RunIdIncrementer()).flow(masterStep()).end().build();
	}
	
	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(4);
		taskExecutor.setCorePoolSize(4);
		taskExecutor.setQueueCapacity(4);
		return taskExecutor;
	}
}
