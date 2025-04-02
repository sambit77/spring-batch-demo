package com.example.spring_batch_demo.config;

import com.example.spring_batch_demo.component.EmployeeProcessor;
import com.example.spring_batch_demo.component.EmployeeWriter;
import com.example.spring_batch_demo.entity.Employee;
import com.example.spring_batch_demo.partition.CustomPartitioner;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    @Value("classpath:/org/springframework/batch/core/schema-drop-postgresql.sql")
    private Resource dropRepositoryTables;

    @Value("classpath:/org/springframework/batch/core/schema-postgresql.sql")
    private Resource dataRepositorySchema;

    @Bean
    public FlatFileItemReader<Employee> reader()
    {
        FlatFileItemReader<Employee> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/employee_data.csv")); //set the path for input file
        itemReader.setName("employeeReader"); //setting reader component name
        itemReader.setLinesToSkip(1); //how many lines to skip from top
        itemReader.setLineMapper(lineMapper());

        return itemReader;
    }

    private LineMapper<Employee> lineMapper() {
        DefaultLineMapper<Employee> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(","); //setting comma as delimiter
        lineTokenizer.setStrict(false); //dalse meanse a line can still be processed even if dome values missing
        lineTokenizer.setNames("id","name","username","gender","salary");

        BeanWrapperFieldSetMapper<Employee> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Employee.class); //mapping with entity (entity variables are same as csv headers

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    public EmployeeProcessor processor()
    {
        return  new EmployeeProcessor();
    }
    @Bean
    public EmployeeWriter writer()
    {
        return new EmployeeWriter();
    }

    /*@Bean
    @Transactional
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager)
    {
        return  new StepBuilder("step1",jobRepository).<Employee,Employee> chunk(10,transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }*/

    // defining master step - it will first create the partitions and then send the process to slave step simultaneously
    @Bean
    public Step masterStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner(slaveStep1(jobRepository, transactionManager).getName(), partitioner())
                .partitionHandler(partitionHandler(jobRepository, transactionManager))
                .build();
    }


    @Bean
    public Step slaveStep1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("slaveStep1", jobRepository).<Employee, Employee> chunk(10, transactionManager) // means 1 chunk is of 10 size
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean(name = "batchJob")
    public Job job(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager)
    {
        return new JobBuilder("importEmployeeFromCSVToDB",jobRepository)
                .preventRestart() //set flag to prevent re execution
                .start(masterStep(jobRepository,platformTransactionManager))//start step-1
                //.next(nextStep) //to call next steps
                .build();
    }

    @Bean(name = "transactionManager")
    public PlatformTransactionManager getTransactionManager()
    {
        return new JpaTransactionManager();
    }

    @Bean(name = "jobRepository")
    public JobRepository getJobRepository() throws Exception
    {
        JobRepositoryFactoryBean factoryBean = new JobRepositoryFactoryBean();
        factoryBean.setDataSource(postgresDataSource());
        factoryBean.setTransactionManager(getTransactionManager());
        factoryBean.afterPropertiesSet();
        return  factoryBean.getObject();
    }

    public DataSource postgresDataSource()
    {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:15432/postgres");
        dataSource.setUsername("postgres");
        dataSource.setPassword("password");
        dataSourceInitializer(dataSource);

        return dataSource;
    }

    @Bean
    public DataSourceInitializer dataSourceInitializer(DataSource dataSource) {
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();
        databasePopulator.addScript(dropRepositoryTables);
        databasePopulator.addScript(dataRepositorySchema);
        databasePopulator.setIgnoreFailedDrops(false);

        DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
        dataSourceInitializer.setDataSource(dataSource);
        dataSourceInitializer.setDatabasePopulator(databasePopulator);

        return dataSourceInitializer;
    }

    @Bean
    public CustomPartitioner partitioner() {
        return new CustomPartitioner();
    }

    public PartitionHandler partitionHandler(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setGridSize(2); // it will create 2 partition of 1-500 and 501 to 1000
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(slaveStep1(jobRepository, transactionManager));

        return taskExecutorPartitionHandler;
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
