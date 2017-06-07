package org.wso2.extensions.siddhi.io.file;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by minudika on 25/5/17.
 */
public class FileProcessor {
    FileObject fileObject;
    ArrayList<Object> messageHolder;
    SourceEventListener sourceEventListener;
    boolean isFileTailingEnabled;
    long filePointer = 0;
    String fileURI;
    ExecutionPlanContext executionPlanContext;
    public FileProcessor(ExecutionPlanContext executionPlanContext, SourceEventListener sourceEventListener,FileObject fileObject,boolean isFileTailingEnabled){
        this.fileObject = fileObject;
        this.sourceEventListener = sourceEventListener;
        this.isFileTailingEnabled = isFileTailingEnabled;
        this.executionPlanContext = executionPlanContext;
        fileURI = fileObject.getName().getURI();
        messageHolder = new ArrayList<Object>();
    }

    public void process(){
        try {
            InputStream inputStream = fileObject.getContent().getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                messageHolder.add(line);
                Thread.sleep(100);
                sourceEventListener.onEvent(line);
            }
        } catch (FileSystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processWithTailing(){
        RandomAccessContent rac = null;
        try {
            while(true){
                Thread.sleep(100);
                long len = fileObject.getContent().getSize();
                if (len < filePointer) {
                    System.out.println("Log file was reset. Restarting logging from start of file.");
                    filePointer = len;
                }
                else if (len > filePointer) {
                    // File must have had something added to it!
                    rac = fileObject.getContent().getRandomAccessContent(RandomAccessMode.READ);
                    rac.seek(filePointer);
                    InputStream inputStream = rac.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    String line = null;
                    while ((line = bufferedReader.readLine()) != null) {
                        //System.out.println(line);
                        sourceEventListener.onEvent(line);
                        Thread.sleep(100);
                    }
                    filePointer = rac.getFilePointer();
                    rac.close();
                }
            }
        } catch (FileSystemException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        if(isFileTailingEnabled) {
            processWithTailing();
        }else{
            process();
        }
    }
}
