package com.example.spring_batch_demo.component.listener;

import com.example.spring_batch_demo.entity.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

@Component
public class CustomSkipListener implements SkipListener<Employee, Number> {
    Logger logger = LoggerFactory.getLogger(CustomSkipListener.class);
    @Override
    public void onSkipInRead(Throwable t) {
        //SkipListener.super.onSkipInRead(t);
        logger.info("A failure in read "+t.getMessage());

    }

    @Override
    public void onSkipInWrite(Number item, Throwable t) {
       // SkipListener.super.onSkipInWrite(item, t);
        logger.info("A failure in write "+t.getMessage()+" "+item);
    }

    @SneakyThrows
    @Override
    public void onSkipInProcess(Employee item, Throwable t) {
        //SkipListener.super.onSkipInProcess(item, t);
        try{
        logger.info("Item "+new ObjectMapper().writeValueAsString(item)
        +" was skipped due to exception "+t.getMessage()); }
        catch (Exception e){
            System.out.println(e.getMessage());
        };
    }
}
