package com.mum.edu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable<PairWritable>{
	
	private Integer key;
	private Integer value;
	
	public PairWritable(){}
	
	public PairWritable(Integer key, Integer value){
		setKey(key);
		setValue(value);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		key = input.readInt();
		value = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeInt(key);
		output.writeInt(value);
	}
	
	

	@Override
	public int hashCode() {
		return Objects.hash(this.key,this.value);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof PairWritable))
		      return false;
		PairWritable other = (PairWritable)obj;
		return this.key == other.key;
	}

	@Override
	public int compareTo(PairWritable o) {
		// TODO Auto-generated method stub
		return (this.key<o.key ? -1 :(this.key == o.key ? 0 : 1));
	}

	public Integer getKey() {
		return key;
	}

	public void setKey(Integer key) {
		this.key = key;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	
}
