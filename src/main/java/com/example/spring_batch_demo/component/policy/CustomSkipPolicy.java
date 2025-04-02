package com.example.spring_batch_demo.component.policy;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;

public class CustomSkipPolicy implements SkipPolicy {
    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        //you can skipCount too for some check
        return t instanceof FlatFileParseException; //i ftrue means want to apply skip
    }
}
