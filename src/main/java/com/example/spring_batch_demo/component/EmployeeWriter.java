package com.example.spring_batch_demo.component;

import com.example.spring_batch_demo.entity.Employee;
import com.example.spring_batch_demo.repository.EmployeeRepository;
import jakarta.persistence.EntityManager;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class EmployeeWriter implements ItemWriter<Employee> {
    @Autowired
    private EmployeeRepository employeeRepository;

    @Autowired
    private EntityManager entityManager;

    @Override
    @Transactional
    public void write(Chunk<? extends Employee> chunk) throws Exception {

        System.out.println("Thread name: " + Thread.currentThread().getName());
        //System.out.println(chunk.getItems().toString());
        employeeRepository.saveAll(chunk.getItems());



    }

}
