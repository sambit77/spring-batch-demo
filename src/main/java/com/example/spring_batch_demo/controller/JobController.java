package com.example.spring_batch_demo.controller;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @PostMapping("/employee")
    public void importCsvToDBJob()
    {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("startAt" , System.currentTimeMillis()).toJobParameters(); //adding jobparams

            try {
                jobLauncher.run(job,jobParameters); //running job -> entry point for spring-batch -> This will look for Job bean
            } catch (JobExecutionAlreadyRunningException | JobParametersInvalidException |
                     JobInstanceAlreadyCompleteException | JobRestartException e) {
               e.printStackTrace();
            }

    }
}
