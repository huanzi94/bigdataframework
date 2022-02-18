package com.orkasgb.hadoop.mapperreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortEntity implements WritableComparable<SortEntity> {

    String no;
    String name;
    String classes;
    int chiness;
    int math;
    int english;
    int history;
    int physics;
    int totle;

    public SortEntity() {
    }

    @Override
    public int compareTo(SortEntity o) {
        if (this.chiness > o.getChiness()) {
            return -1;
        } else if (this.chiness < o.getChiness()) {
            return 1;
        } else {
            if (this.math > o.getMath()) {
                return -1;
            } else if (this.math < o.getMath()) {
                return 1;
            }
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(no);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(classes);
        dataOutput.writeInt(chiness);
        dataOutput.writeInt(math);
        dataOutput.writeInt(english);
        dataOutput.writeInt(history);
        dataOutput.writeInt(physics);
        dataOutput.writeInt(totle);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.no = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.classes = dataInput.readUTF();
        this.chiness = dataInput.readInt();
        this.math = dataInput.readInt();
        this.english = dataInput.readInt();
        this.history = dataInput.readInt();
        this.physics = dataInput.readInt();
        this.totle = dataInput.readInt();
    }

    @Override
    public String toString() {
        return  no + '\t' + name + '\t' + classes + '\t' +
                chiness + "\t" + math +
                "\t" + english +
                "\t" + history +
                "\t" + physics +
                "\t" + totle;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClasses() {
        return classes;
    }

    public void setClasses(String classes) {
        this.classes = classes;
    }

    public int getChiness() {
        return chiness;
    }

    public void setChiness(int chiness) {
        this.chiness = chiness;
    }

    public int getMath() {
        return math;
    }

    public void setMath(int math) {
        this.math = math;
    }

    public int getEnglish() {
        return english;
    }

    public void setEnglish(int english) {
        this.english = english;
    }

    public int getHistory() {
        return history;
    }

    public void setHistory(int history) {
        this.history = history;
    }

    public int getPhysics() {
        return physics;
    }

    public void setPhysics(int physics) {
        this.physics = physics;
    }

    public int getTotle() {
        return this.chiness + this.math + this.english + this.history + this.physics;
    }

    public void setTotle(int totle) {
        this.totle = totle;
    }

    public void setTotle() {
        this.totle = this.chiness + this.math + this.english + this.history + this.physics;
    }
}
