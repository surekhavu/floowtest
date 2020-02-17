package com.floow.commonleastwordlister.batchprocessing;

import com.floow.commonleastwordlister.batchprocessing.model.Words;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    @Autowired
    MongoTemplate mongoTemplate;

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to View the results");
            Words commonWord = getSortedWord(Sort.Direction.DESC);
            log.info("The most common word is <<<<<<<<<<<<<<<<" + commonWord.getWord() + ">>>>>>>>>>>>>>> occurred...... " + commonWord.getOccurences());

            Words leastWord = getSortedWord(Sort.Direction.ASC);
            log.info("The least word is <<<<<<<<<<<<<<<<" + leastWord.getWord() + ">>>>>>>>>>>>>>> occurred...... " + leastWord.getOccurences());
        }
    }

    private Words getSortedWord(Sort.Direction sort) {
        Query query = new Query();
        query.with(Sort.by(sort, "occurences"));
        query.limit(1);
        Words word = mongoTemplate.findOne(query, Words.class);
        return word;
    }

}
