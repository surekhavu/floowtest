package com.floow.commonleastwordlister.batchprocessing.model;

public class Words {
    private  String word=null;
    private  int occurences=0;

    public Words(String word, int occurences) {
        this.word = word;
        this.occurences = occurences;
    }

    public String getWord() {
        return word;
    }

    public int getOccurences() {
        return occurences;
    }

    public void setWord(final String word) {
        this.word=word;
    }

    public void setOccurences() {
        this.occurences=occurences;
    }
}
