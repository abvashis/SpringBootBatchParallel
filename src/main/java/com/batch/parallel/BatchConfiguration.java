package com.batch.parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
 
import java.net.MalformedURLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;
 
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
 
    Logger logger = LoggerFactory.getLogger(BatchConfiguration.class);
 
    @Autowired
    JobBuilderFactory jobBuilderFactory;
 
    @Autowired
    StepBuilderFactory stepBuilderFactory;
    
    //We have configured a simple TaskletStep. 
    // The step includes a Tasklet which iterates from numbers 1 to 100 and  prints to the console. 
    // In the tasklet, we return RepeatStatus.FINISHED to indicate successful execution.
 
    private TaskletStep taskletStep(String step) {
        return stepBuilderFactory.get(step).tasklet((contribution, chunkContext) -> {
            IntStream.range(1, 100).forEach(token -> logger.info("Step:" + step + " token:" + token));
            return RepeatStatus.FINISHED;
        }).build();
 
    }
 
    //We are parallelizing multiple jobs. For our example, each job is going to use the simple Tasklet
    @Bean
    public Job parallelStepsJob() {
 
        Flow masterFlow = (Flow) new FlowBuilder("masterFlow").start(taskletStep("step1")).build();
 
 
        Flow flowJob1 = (Flow) new FlowBuilder("flow1").start(taskletStep("step2")).build();
        Flow flowJob2 = (Flow) new FlowBuilder("flow2").start(taskletStep("step3")).build();
        Flow flowJob3 = (Flow) new FlowBuilder("flow3").start(taskletStep("step4")).build();
 
        /**
         * A simple SlaveFlow is configured to hold all three flow jobs. 
         * We configure the SlaveFlow with a SimpleAsyncTaskExecutor that executes multiple threads parallelly. 
         * We have not defined a thread pool, so Spring will keep spawning threads to match the jobs provided. 
         * This ensures the parallel execution of jobs configured. 
         * There are multiple TaskExecutor implementations available, but AsyncTaskExecutor ensures that the 
         * tasks are executed in parallel. 
         * AsyncTaskExecutor has a concurrencyLimit property which can be used to throttle the number of threads 
         * executing parallelly.
         */
        Flow slaveFlow = (Flow) new FlowBuilder("slaveFlow")
                .split(new SimpleAsyncTaskExecutor()).add(flowJob1, flowJob2, flowJob3).build();
 
        return (jobBuilderFactory.get("parallelFlowJob")
                .incrementer(new RunIdIncrementer())
                .start(masterFlow)
                .next(slaveFlow)
                .build()).build();
 
    }
 
}