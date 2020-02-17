package com.floow.commonleastwordlister;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CommonLeastWordListerApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(CommonLeastWordListerApplication.class);

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job job;

    public static void main(String[] args) {

        Options options = new Options();

        Option source = new Option("s", "source", true, "source file path");
        source.setRequired(true);
        options.addOption(source);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.info(e.getMessage());
            formatter.printHelp("CommonLeastWordListerApplication", options);

            System.exit(1);
        }
        String[] arguments = new String[]{cmd.getOptionValue("source")};

        SpringApplication.run(CommonLeastWordListerApplication.class, arguments);

    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Started the job ");

        JobParametersBuilder jobBuilder = new JobParametersBuilder()
                .addString("source", args[0]);

        jobLauncher.run(job, jobBuilder.toJobParameters());
    }
}