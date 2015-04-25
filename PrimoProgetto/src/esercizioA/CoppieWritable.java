package esercizioA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CoppieWritable implements WritableComparable<CoppieWritable> {

	private Text leftPair;
	private Text rightPair;
	

	public CoppieWritable(){
	}
	
	public CoppieWritable(Text left,Text right){
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
		if(o instanceof CoppieWritable){
			CoppieWritable pair = (CoppieWritable) o;
			if (leftPair.equals(pair.leftPair))
				return rightPair.equals(pair.rightPair);
		}
		return false;
	}
	
	@Override
	public int compareTo(CoppieWritable p) {
		int cmp = leftPair.compareTo(p.leftPair);
		if(cmp==0)
			return rightPair.compareTo(p.rightPair);
		else{
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
