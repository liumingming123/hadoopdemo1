package com.test.hadoop.po;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class MySort implements WritableComparable<MySort> {
	private static  final Logger log= LoggerFactory.getLogger(MySort.class);
	public String text;
	public long num;

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public long getNum() {
		return num;
	}

	public void setNum(long num) {
		this.num = num;
	}

	public MySort(){

	}

	public MySort(String text, long num) {
		this.text = text;
		this.num = num;
	}

	public int compareTo(MySort o) {
		return (int)(o.getNum()-num);


	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(text);
		dataOutput.writeLong(num);
	}

	public void readFields(DataInput dataInput) throws IOException {
		this.text=dataInput.readUTF();
		this.num=dataInput.readLong();
	}
}
