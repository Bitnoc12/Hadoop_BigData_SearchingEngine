package com.bingh.mapreduce.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

public class InvertedIndexReducer extends Reducer<Text, Word, Text, Text> {

    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Word> values, Reducer<Text, Word, Text, Text>.Context context) throws IOException, InterruptedException {

        // 创建一个HashSet集合，集合中的每个元素类型为Long类型
        // HashSet是一个不允许有重复元素的集合
        // 所以，在同一篇文档中如果出现了多次相同的单词时，
        // 那么这些出现在同一篇文档中的单词 在遍历它们所在的文档id添加到HashSet的时侯
        // 相同的文档id就不会再重复地添加进HashSet里
        // 那么，HashSet只能保证文档id的不重复性以及唯一性
        // 如果单词是在不同的文档中出现，那么不同的文档id就可以被添加进HashSet里
        // 最终HashSet存储的是单词所在的（不重复）文档id的集合
        // HashSet<Long> docIdHashSet = new HashSet<>();

        // 遍历单词的 Word 对象
        // "key": ( Word (单词所在的文档),(单词在文档中的位置) )
        // "a": ( (0,2), (0,4), (1,2) )

        // output
        // key  Text
        // "a": docNum (0,2,<2,4>), (1,1,<2>)

        long currentId = -1;
        long docNum = 0;
        long wordNum = 0;
        String posList = "";

        Doc doc = null;

        ArrayList<Doc> arrayList = new ArrayList<>();

        for (Word value : values) {

            if (currentId == value.getDocId()) {
                wordNum++;
                posList += "," + value.getWordPos();
            }

            else {

                if (currentId == -1) {
                    docNum++;
                    doc = new Doc();
                    doc.setDocId(value.getDocId());
                    wordNum = 1;
                    posList = "<" + value.getWordPos();
                    currentId = value.getDocId();
                }

                else {
                    docNum++;
                    doc.setWordNum(wordNum);
                    posList += ">";
                    doc.setPosList(posList);
                    arrayList.add(doc);

                    doc = new Doc();

                    doc.setDocId(value.getDocId());
                    wordNum = 1;
                    posList = "<" + value.getWordPos();

                    currentId = value.getDocId();
                }

            }

        }

        doc.setWordNum(wordNum);
        posList += ">";
        doc.setPosList(posList);
        arrayList.add(doc);

        // 根据Doc对象的wordNum属性对arrayList进行排序
        // 使用Collections.sort()方法
        Collections.sort(arrayList, new Comparator<Doc>() {
            @Override
            public int compare(Doc doc1, Doc doc2) {
                // 首先按照 wordNum 逆序排序
                int wordNumComparison =  Long.compare(doc2.getWordNum(), doc1.getWordNum());

                // 如果 wordNum 相等，再按照 docId 顺序排序
                if (wordNumComparison == 0) {
                    return Long.compare(doc1.getDocId(), doc2.getDocId());
                }

                return wordNumComparison;
            }
        });

        // 创建一个StringBuilder对象，用于构建字符串
        StringBuilder docIDsBuilder = new StringBuilder();

        // 遍历docIDSet集合中的每一个docID，把当前docID的值(Long类型)转换为字符串，再将其添加到docIDsBuilder中
        // (意义: 将docIDSet集合转换为以逗号(",")分隔的StringBuilder对象）
//        for (Long docID : docIdHashSet) {
//            docIDsBuilder.append(docID).append(",");
//        }

        docIDsBuilder.append(docNum).append(" ");

        for (Doc array : arrayList) {
            docIDsBuilder.append("(").append(array).append("), ");
        }

        // 移除字符串末尾的逗号和space
        if (docIDsBuilder.length() > 0) {
            docIDsBuilder.setLength(docIDsBuilder.length() - 2);
        }

        // 首先将StringBuilder转换为String，再由String转换为Text类型
       outValue.set(docIDsBuilder.toString());

        context.write(key, outValue);     // 写出

    }

}
