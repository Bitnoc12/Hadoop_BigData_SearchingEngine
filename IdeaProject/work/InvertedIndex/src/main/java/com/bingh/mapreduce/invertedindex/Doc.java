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

public class Doc implements Writable {

    private long docId;
    private long wordNum;
    private String posList;

    public Doc() {
    }

    public long getDocId() {
        return docId;
    }

    public void setDocId(long docId) {
        this.docId = docId;
    }

    public long getWordNum() {
        return wordNum;
    }

    public void setWordNum(long wordNum) {
        this.wordNum = wordNum;
    }

    public String getPosList() {
        return posList;
    }

    public void setPosList(String posList) {
        this.posList = posList;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(docId);
        dataOutput.writeLong(wordNum);
        dataOutput.writeChars(posList);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.docId = dataInput.readLong();
        this.wordNum = dataInput.readLong();
        this.posList = dataInput.readLine();
    }

    @Override
    public String toString() {
        return docId + "," + wordNum + "," + posList;
    }

}
