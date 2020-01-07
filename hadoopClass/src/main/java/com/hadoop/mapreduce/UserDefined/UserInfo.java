package com.hadoop.mapreduce.UserDefined;

import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义UserInfo的输入格式，作为输入类型
 */
public class UserInfo implements WritableComparable<UserInfo> {
    //定义user的属性
    private int id;
    private String name;
    private int age;
    private String sex;
    private String address;

    public UserInfo() {
    }

    //UserInfo的构造函数
    public UserInfo(int id, String name, int age, String sex, String address) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.address = address;
    }

    //成员方法
    public int compareTo(UserInfo o) {
        return 0;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        out.writeInt(age);
        out.writeUTF(sex);
        out.writeUTF(address);
    }

    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.name = in.readUTF();
        this.age = in.readInt();
        this.sex = in.readUTF();
        this.address = in.readUTF();
    }

    @Override
    public String toString() {
        return "Id:" + id + ", Name:" + name + ", Age:" + age + ", Sex:" + sex + ", Address:" + address ;
    }

    public static void main(String[] args) {
        UserInfo user = new UserInfo(123, "小五", 10, "男", "***");

        System.out.println(user);
    }
}
