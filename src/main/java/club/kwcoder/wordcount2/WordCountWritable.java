package club.kwcoder.wordcount2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCountWritable implements WritableComparable<WordCountWritable> {

    // 定义属性
    private String word;
    private Integer count;


    /**
     * 重写比较器，用于Map阶段的排序
     *
     * @param o the object to be compared.
     * @return
     */
    public int compareTo(WordCountWritable o) {
        return this.word.compareTo(o.getWord());
    }

    /**
     * Hadoop序列化
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.word);
        out.writeInt(this.count);
    }

    /**
     * Hadoop 反序列化
     * ！！！注意：！！！先被序列化的先反序列化
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // ！！！注意：！！！先被序列化的先反序列化
        this.word = in.readUTF();
        this.count = in.readInt();
    }

    public WordCountWritable(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public WordCountWritable() {
    }

    @Override
    public String toString() {
        return word + '\t' + count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordCountWritable that = (WordCountWritable) o;

        if (!word.equals(that.word)) return false;
        return count.equals(that.count);
    }

    @Override
    public int hashCode() {
        int result = word.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

}
