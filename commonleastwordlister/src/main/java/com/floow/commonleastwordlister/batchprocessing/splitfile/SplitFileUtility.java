package com.floow.commonleastwordlister.batchprocessing.splitfile;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SplitFileUtility {

    // no of files to split
    public static final long NUM_SPLITS = 10;
    public static final String SPLIT = "split_";
    public static final String FILE_EXTENSION = ".txt";

    public static List<String> splitFile(final String fileName)throws IOException
    {
        RandomAccessFile raf = new RandomAccessFile(fileName, "r");
        long sourceSize = raf.length();
        long bytesPerSplit = sourceSize/ NUM_SPLITS;
        long remainingBytes = sourceSize % NUM_SPLITS;
        List<String> splitFile = new ArrayList<String>();
        int maxReadBufferSize = 8 * 1024; //8KB
        for(int destIx = 1; destIx <= NUM_SPLITS; destIx++) {
            String splitFileName = SPLIT +destIx+ FILE_EXTENSION;
            File file = new File(splitFileName);
            splitFile.add(file.getAbsolutePath());
            FileOutputStream fileOutputStream = new FileOutputStream(file);

            BufferedOutputStream bw = new BufferedOutputStream(fileOutputStream);
            if(bytesPerSplit > maxReadBufferSize) {
                long numReads = bytesPerSplit/maxReadBufferSize;
                long numRemainingRead = bytesPerSplit % maxReadBufferSize;
                for(int i=0; i<numReads; i++) {
                    readWrite(raf, bw, maxReadBufferSize);
                }
                if(numRemainingRead > 0) {
                    readWrite(raf, bw, numRemainingRead);
                }
            }else {
                readWrite(raf, bw, bytesPerSplit);
            }
            bw.close();
        }
        if(remainingBytes > 0) {
            String splitFileName = SPLIT+(NUM_SPLITS +1)+ FILE_EXTENSION;
            File file = new File(splitFileName);
            splitFile.add(file.getAbsolutePath());
            BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file));
            readWrite(raf, bw, remainingBytes);
            bw.close();
        }
        raf.close();
        return splitFile;
    }

    private static void readWrite(RandomAccessFile raf, BufferedOutputStream bw, long numBytes) throws IOException {
        byte[] buf = new byte[(int) numBytes];
        int val = raf.read(buf);
        if(val != -1) {
            bw.write(buf);
        }
    }


}
