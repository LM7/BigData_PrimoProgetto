package esercizioB;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class EnneWritable implements WritableComparable<EnneWritable>{

	private Text firstValue;
	private Text secondoValue;
	private Text thirdValue;
	private Text fourthValue;

	public EnneWritable(){
	}

	public EnneWritable(Text firstValue, Text secondValue, Text thirdValue, Text fourthValue){
		this.firstValue = firstValue;
		this.secondoValue = secondValue;
		this.thirdValue = thirdValue;
		this.fourthValue = fourthValue;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(firstValue.toString());
		out.writeUTF(secondoValue.toString());
		out.writeUTF(thirdValue.toString());
		out.writeUTF(fourthValue.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstValue = new Text(in.readUTF());
		secondoValue = new Text(in.readUTF());
		thirdValue = new Text(in.readUTF());
		fourthValue = new Text(in.readUTF());
	}

	public void set(Text[] values) {
		firstValue=values[0];
		secondoValue=values[1];
		thirdValue = values[2];
		fourthValue = values[3];

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((fourthValue == null) ? 0 : fourthValue.hashCode());
		result = prime * result
				+ ((firstValue == null) ? 0 : firstValue.hashCode());
		result = prime * result
				+ ((secondoValue == null) ? 0 : secondoValue.hashCode());
		result = prime * result
				+ ((thirdValue == null) ? 0 : thirdValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EnneWritable other = (EnneWritable) obj;
		if (fourthValue == null) {
			if (other.fourthValue != null)
				return false;
		} else if (!fourthValue.equals(other.fourthValue))
			return false;
		if (firstValue == null) {
			if (other.firstValue != null)
				return false;
		} else if (!firstValue.equals(other.firstValue))
			return false;
		if (secondoValue == null) {
			if (other.secondoValue != null)
				return false;
		} else if (!secondoValue.equals(other.secondoValue))
			return false;
		if (thirdValue == null) {
			if (other.thirdValue != null)
				return false;
		} else if (!thirdValue.equals(other.thirdValue))
			return false;
		return true;
	}
	
	
	@Override
	public int compareTo(EnneWritable o) {
		
		Text[] valuesThis = new Text[]{firstValue,secondoValue,thirdValue,fourthValue};
		Text[] valuesThat = new Text[]{o.firstValue,o.secondoValue,o.thirdValue,o.fourthValue};		
		Arrays.sort(valuesThis);
		Arrays.sort(valuesThat);
				
		this.set(valuesThis);
		o.set(valuesThat);
		
		int cmp = firstValue.compareTo(o.firstValue);
        if (cmp != 0) {
            return cmp;
        }
        int cmp2= secondoValue.compareTo(o.secondoValue);
        if (cmp2 != 0) {
            return cmp2;
        }
        int cmp3= thirdValue.compareTo(o.thirdValue);
        if (cmp3 != 0) {
            return cmp3;
        }
        int cmp4= fourthValue.compareTo(o.fourthValue);
        if (cmp4 != 0) {
            return cmp4;
        }
		return cmp4;
	}

	@Override
	public String toString() {
		return firstValue.toString()+" "+secondoValue.toString()+" "+thirdValue.toString()+" "+fourthValue.toString();
	}

	public Text getLeftPair() {
		return firstValue;
	}

	public Text getRightPair() {
		return secondoValue;
	}

	public Text getThirdValue() {
		return thirdValue;
	}

	public Text getFourthValue() {
		return fourthValue;
	}
}
