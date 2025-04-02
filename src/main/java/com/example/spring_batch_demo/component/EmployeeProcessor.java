package com.example.spring_batch_demo.component;

import com.example.spring_batch_demo.entity.Employee;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EmployeeProcessor implements ItemProcessor<Employee,Employee> {
    @Override
    public Employee process(Employee emp) throws Exception {
        Long salary = emp.getSalary();

        if(salary >= 50000)
        {
            return  emp;
        }
        return null;
    }
}
