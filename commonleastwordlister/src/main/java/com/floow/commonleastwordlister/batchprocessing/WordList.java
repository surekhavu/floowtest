package com.floow.commonleastwordlister.batchprocessing;


import com.floow.commonleastwordlister.batchprocessing.model.Words;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * From a Word line, constructing list of words
 */
public class WordList {

    private static final  String WORD_PATTERN_EXPRESSION = "([^\\W_]+(?:['_-][^\\W_]+)*)";
    private Collection words;

    public WordList(String wordsLine) {

        words = new ArrayList<Words>();

        Matcher matcher = Pattern.compile(WORD_PATTERN_EXPRESSION).matcher(wordsLine);
        while (matcher.find()) {
            ((ArrayList) words).add(new Words(matcher.group(), 0));
        }
    }

    public Collection<Words> getWords() {
        return words;
    }

}
