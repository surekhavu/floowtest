package com.floow.commonleastwordlister.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class WordsItemProcessor  implements ItemProcessor<WordLine, WordList> {

    private static final Logger log = LoggerFactory.getLogger(WordsItemProcessor.class);

    @Override
    public WordList process(final WordLine wordLine) throws Exception {

        final WordList processedWordList = new WordList(wordLine.getWords());

        return processedWordList;
    }
}