package com.neuedu.hdfs_demo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Student implements WritableComparable<Student> {

    private Integer id;

    private String name;

    private Float score;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // hadoop序列化
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.name);
        dataOutput.writeFloat(this.score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // hadoop反序列化
        this.id = dataInput.readInt();
        this.name = dataInput.readUTF();
        this.score = dataInput.readFloat();
    }

    @Override
    public int compareTo(Student o) {
        if (null == o) {
            return 1;
        }
        return -Float.compare(this.score, o.score);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Student student = (Student) o;

        if (!id.equals(student.id)) return false;
        if (!name.equals(student.name)) return false;
        return score.equals(student.score);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + score.hashCode();
        return result;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Float getScore() {
        return score;
    }

    public void setScore(Float score) {
        this.score = score;
    }

    public Student(Integer id, String name, Float score) {
        this.id = id;
        this.name = name;
        this.score = score;
    }

    public Student() {
    }
}
