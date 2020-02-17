package com.floow.commonleastwordlister.batchprocessing.data;

import com.floow.commonleastwordlister.batchprocessing.WordLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Customized class of FlatFileItemReader, not to separate fields by a delimiter just read the line
 * @param <T> WordLine
 */
public class WordLineItemReader<T> extends FlatFileItemReader<T> {

    private static final Log logger = LogFactory.getLog(WordLineItemReader.class);
    public static final String DEFAULT_CHARSET = Charset.defaultCharset().name();
    private RecordSeparatorPolicy recordSeparatorPolicy = new SimpleRecordSeparatorPolicy();
    private Resource resource;
    private BufferedReader reader;
    private int lineCount = 0;
    private boolean noInput;
    private String encoding;
    private LineMapper<T> lineMapper;
    private BufferedReaderFactory bufferedReaderFactory;

    public WordLineItemReader() {
        this.noInput = false;
        this.encoding = DEFAULT_CHARSET;
        this.bufferedReaderFactory = new DefaultBufferedReaderFactory();
        this.setName(ClassUtils.getShortName(WordLineItemReader.class));
    }

    public void setLineMapper(LineMapper<T> lineMapper) {
        this.lineMapper = lineMapper;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setBufferedReaderFactory(BufferedReaderFactory bufferedReaderFactory) {
        this.bufferedReaderFactory = bufferedReaderFactory;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public void setRecordSeparatorPolicy(RecordSeparatorPolicy recordSeparatorPolicy) {
        this.recordSeparatorPolicy = recordSeparatorPolicy;
    }

    //only this method is customized
    @Override
    @Nullable
    protected T doRead() throws Exception {
        if (this.noInput) {
            return null;
        } else {
            String line = this.readLine();
            if (line == null) {
                return null;
            } else {
                WordLine wordLine = new WordLine(line);
                return (T) wordLine;
            }
        }
    }

    @Nullable
    private String readLine() {
        if (this.reader == null) {
            throw new ReaderNotOpenException("Reader must be open before it can be read.");
        } else {
            String line = null;

            try {
                line = this.reader.readLine();
                if (line == null) {
                    return null;
                } else {
                    return line;
                }
            } catch (IOException var3) {
                this.noInput = true;
                throw new NonTransientFlatFileException("Unable to read from resource: [" + this.resource + "]", var3, line, this.lineCount);
            }
        }
    }

    protected void doClose() throws Exception {
        this.lineCount = 0;
        if (this.reader != null) {
            this.reader.close();
        }

    }

    protected void doOpen() throws Exception {
        Assert.notNull(this.resource, "Input resource must be set");
        Assert.notNull(this.recordSeparatorPolicy, "RecordSeparatorPolicy must be set");
        if (!this.resource.exists()) {
            throw new IllegalStateException("Input resource must exist : " + this.resource);
        } else if (!this.resource.isReadable()) {
            throw new IllegalStateException("Input resource must be readable : " + this.resource);
        } else {
            this.reader = this.bufferedReaderFactory.create(this.resource, this.encoding);
        }
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(this.lineMapper, "LineMapper is required");
    }

}
