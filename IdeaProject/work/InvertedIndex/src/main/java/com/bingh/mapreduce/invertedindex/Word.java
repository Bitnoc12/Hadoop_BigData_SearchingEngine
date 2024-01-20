package com.bingh.mapreduce.invertedindex;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1. 定义一个类实现writable接口
 * 2. 重写序列化和反序列化方法
 * 3. 重写空参构造
 * 4. toString方法
 */

public class Word implements Writable {

    private long docId;     // 单词所在的文档
    private long wordPos;   // 单词在文档中的位置

    public Word() {
    }

    public long getDocId() {
        return docId;
    }

    public void setDocId(long docId) {
        this.docId = docId;
    }

    public long getWordPos() {
        return wordPos;
    }

    public void setWordPos(long wordPos) {
        this.wordPos = wordPos;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(docId);
        dataOutput.writeLong(wordPos);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.docId = dataInput.readLong();
        this.wordPos = dataInput.readLong();
    }

    @Override
    public String toString() {
        return docId + " " + wordPos;
    }

}
