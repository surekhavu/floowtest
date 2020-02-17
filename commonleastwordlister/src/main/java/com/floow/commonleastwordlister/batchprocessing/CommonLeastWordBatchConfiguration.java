package com.floow.commonleastwordlister.batchprocessing;

import com.floow.commonleastwordlister.batchprocessing.constants.CommonLeastWordConstants;
import com.floow.commonleastwordlister.batchprocessing.data.WordLineItemReader;

import com.floow.commonleastwordlister.batchprocessing.data.WordWriter;
import com.floow.commonleastwordlister.batchprocessing.splitfile.SplitFileUtility;
import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

// tag::setup[]
@Configuration
@EnableBatchProcessing
public class CommonLeastWordBatchConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CommonLeastWordBatchConfiguration.class);

    private static final String OVERRIDDEN_BY_EXPRESSION = "VALUE-OVERRIDDEN";
    public static final String WORDS_COLLECTION = "words";
    public static final String WORDS_DB = "wordsdb";
    public static final int MAX_POOL_SIZE = 10;
    public static final int CORE_POOL_SIZE = 10;
    public static final int QUEUE_CAPACITY = 10;
    public static final int CHUNK_SIZE = 100000;


    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MongoItemWriter<WordList> writer;

    @Autowired
    private WordWriter<WordList> wordWriter;

    // end::setup[]

    // tag::readerwriterprocessor[]

    @StepScope

    @Bean
    public Resource getSourceFile(String dest) {
        return new FileSystemResource(dest);
    }


    @Bean("partitioner")
    @StepScope
    public Partitioner partitioner(@Value("#{jobParameters['source']}") String source) {
        log.info("In Partitioner");
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = null;
        try {
            List<String> splitFiles = SplitFileUtility.splitFile(source);
            FileSystemResource[] fileSystemResources = new FileSystemResource[11];
            int counter = 0;
            for (String file : splitFiles) {
                fileSystemResources[counter] = new FileSystemResource(file);
                counter++;
            }
            partitioner.setResources(fileSystemResources);
            partitioner.partition(11);
        } catch (IOException e) {
            log.info("unable to split the file , using the original file");
            FileSystemResource[] fileSystemResource = new FileSystemResource[]{new FileSystemResource(source)};
            partitioner.setResources(fileSystemResource);
            partitioner.partition(1);
        }


        return partitioner;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(MAX_POOL_SIZE);
        taskExecutor.setCorePoolSize(CORE_POOL_SIZE);
        taskExecutor.setQueueCapacity(QUEUE_CAPACITY);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @Bean
    @Qualifier("masterStep")
    public Step masterStep() {
        return stepBuilderFactory.get("masterStep")
                .partitioner("step1", partitioner(OVERRIDDEN_BY_EXPRESSION))
                .step(step1())
                .taskExecutor(taskExecutor())
                .build();
    }


    @Bean
    @StepScope
    @DependsOn("partitioner")
    public WordLineItemReader<WordLine> reader(@Value("#{stepExecutionContext['fileName']}") String filename) {
        final WordLineItemReader<WordLine> reader = new WordLineItemReader<>();
        try {
            reader.setResource(new UrlResource(filename));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        final DefaultLineMapper<WordLine> lineMapper = new DefaultLineMapper<>();
        final DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames(CommonLeastWordConstants.DOCUMENT_FIELD_NAME);
        lineMapper.setLineTokenizer(lineTokenizer);
        final BeanWrapperFieldSetMapper<WordLine> fieldMapper = new BeanWrapperFieldSetMapper<WordLine>();
        fieldMapper.setTargetType(WordLine.class);
        lineMapper.setFieldSetMapper(fieldMapper);
        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    public WordWriter<WordList> writer() {
        WordWriter<WordList> writer = new WordWriter<WordList>();
        try {
            writer.setTemplate(mongoTemplate());

        } catch (Exception e) {
            e.printStackTrace();
        }

        writer.setCollection(WORDS_COLLECTION);
        return writer;
    }
    // end::readerwriterprocessor[]

    @Bean
    public WordsItemProcessor processor() {
        return new WordsItemProcessor();
    }

    // tag::jobstep[]
    @Bean
    public Job commonLeastWordJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("commonLeastWordJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(masterStep())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<WordLine, WordList>chunk(CHUNK_SIZE)
                .reader(reader(OVERRIDDEN_BY_EXPRESSION))
                .processor(processor())
                .writer(wordWriter)
                .build();
    }
    // end::jobstep[]

    @Bean
    public MongoDbFactory mongoDbFactory() throws Exception {
        return new SimpleMongoDbFactory(new MongoClient(), WORDS_DB);
    }

    @Bean
    public MongoTemplate mongoTemplate() throws Exception {
        MongoTemplate mongoTemplate = new MongoTemplate(mongoDbFactory());
        return mongoTemplate;
    }
}
