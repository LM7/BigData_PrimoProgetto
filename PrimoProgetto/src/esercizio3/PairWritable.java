package esercizio3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable<PairWritable> {

	private Text leftPair;
	private Text rightPair;
	

	public PairWritable(){
	}
	
	public PairWritable(Text left,Text right){
		this.leftPair = left;
		this.rightPair = right;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(leftPair.toString());
		out.writeUTF(rightPair.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		leftPair = new Text(in.readUTF());
		rightPair = new Text(in.readUTF());
	}

	public void set(String left,String right){
		leftPair = new Text(left);
		rightPair = new Text(right);
	}
	
	@Override
	public int hashCode(){
		return leftPair.hashCode() + rightPair.hashCode();
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof PairWritable){
			PairWritable pair = (PairWritable) o;
			if (leftPair.equals(pair.leftPair))
				return rightPair.equals(pair.rightPair);
			else
				return leftPair.equals(pair.rightPair) &&
						rightPair.equals(pair.leftPair);
		}
		return false;
	}
	
	@Override
	public int compareTo(PairWritable p) {
		int cmp = leftPair.compareTo(p.leftPair);
		if(cmp==0)
			return rightPair.compareTo(p.rightPair);
		else{
			int cmp2 = leftPair.compareTo(p.rightPair);
			if(cmp2==0){
				return rightPair.compareTo(p.leftPair);
			}
			else
				return cmp;
		}
	}
	
	@Override
	public String toString() {
		return leftPair.toString()+","+rightPair.toString();
	}

	public Text getLeftPair() {
		return leftPair;
	}

	public Text getRightPair() {
		return rightPair;
	}

}
