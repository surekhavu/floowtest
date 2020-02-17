package com.floow.commonleastwordlister.batchprocessing.data;

import com.floow.commonleastwordlister.batchprocessing.WordList;
import com.floow.commonleastwordlister.batchprocessing.constants.CommonLeastWordConstants;
import com.floow.commonleastwordlister.batchprocessing.model.Words;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Customized MongoWriter for doing upsert and as well iterating wordlist for a list of words rather than inserting as wordlist document
 *
 * @param <T>
 */
public class WordWriter<T> extends MongoItemWriter<T> {


    private MongoOperations template;
    private final Object bufferKey = new Object();
    private String collection;

    public WordWriter() {
    }

    public void setTemplate(MongoOperations template) {
        this.template = template;
    }

    protected MongoOperations getTemplate() {
        return this.template;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void write(List<? extends T> items) throws Exception {
        if (!this.transactionActive()) {
            this.doWrite(items);
        } else {
            List<T> bufferedItems = this.getCurrentBuffer();
            bufferedItems.addAll(items);
        }
    }

    /**
     * WordList getwords iterated and upsert word as document rather than wordlist
     * Documents are inserted into mongodb as bulk operations for better performance
     *
     * @param items
     */
    @Override
    protected void doWrite(List<? extends T> items) {
        if (!CollectionUtils.isEmpty(items)) {
            Iterator var2;
            Object object;
            if (StringUtils.hasText(this.collection)) {
                var2 = items.iterator();
                ArrayList<Words> bulkWords = new ArrayList<Words>();
                while (var2.hasNext()) {
                    object = var2.next();
                    if (object instanceof WordList) {
                        WordList wordList = (WordList) object;
                        if (!CollectionUtils.isEmpty(wordList.getWords())) {
                            wordList.getWords().stream().forEach(word -> bulkWords.add(word));
                        }
                    }
                }
                if (!CollectionUtils.isEmpty(bulkWords)) {
                    BulkOperations bulkOperations = template.bulkOps(BulkOperations.BulkMode.UNORDERED, this.collection);
                    List<Pair<Query, Update>> wordPairList = new ArrayList<Pair<Query, Update>>();
                    for (Words word : bulkWords) {
                        wordPairList.add(Pair.of(new Query(Criteria.where(CommonLeastWordConstants.DOCUMENT_FIELD_NAME).is(word.getWord())), new Update().inc(CommonLeastWordConstants.OCCURENCES, 1)));
                    }
                    bulkOperations.upsert(wordPairList);
                    bulkOperations.execute();
                }
            }
        }
    }

    private boolean transactionActive() {
        return TransactionSynchronizationManager.isActualTransactionActive();
    }

    private List<T> getCurrentBuffer() {
        if (!TransactionSynchronizationManager.hasResource(this.bufferKey)) {
            TransactionSynchronizationManager.bindResource(this.bufferKey, new ArrayList());
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
                public void beforeCommit(boolean readOnly) {
                    List<T> items = (List) TransactionSynchronizationManager.getResource(WordWriter.this.bufferKey);
                    if (!CollectionUtils.isEmpty(items) && !readOnly) {
                        WordWriter.this.doWrite(items);
                    }

                }

                public void afterCompletion(int status) {
                    if (TransactionSynchronizationManager.hasResource(WordWriter.this.bufferKey)) {
                        TransactionSynchronizationManager.unbindResource(WordWriter.this.bufferKey);
                    }

                }
            });
        }

        return (List) TransactionSynchronizationManager.getResource(this.bufferKey);
    }

    public void afterPropertiesSet() throws Exception {
        Assert.state(this.template != null, "A MongoOperations implementation is required.");
    }
}
