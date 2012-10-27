import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class SingleRecordWriteable implements WritableComparable<SingleRecordWriteable> {

	private IntWritable rank;
	private Text url;
	private IntWritable did;
	private IntWritable position;

	public SingleRecordWriteable(){
		set(new IntWritable(),new Text(),new IntWritable(),new IntWritable());
	}
	
	public SingleRecordWriteable(IntWritable rank, Text url,IntWritable did, IntWritable position){
		set(rank,url,did,position);
	}

	public IntWritable GetRank()
	{
		return rank;
	}

	public Text GetUrl()
	{
		return url;
	}

	public void set(IntWritable rank, Text url,IntWritable did, IntWritable position)
	{
		this.rank = rank;
		this.url = url;
		this.did = did;
		this.position = position;
	}

	public void setRank(float rank)
	{
		//……
	}

	public void setUrl(Text t_positions)
	{
		//……
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean equals(Object o){
		boolean result = false;
		SingleRecordWriteable t = (SingleRecordWriteable)o;
		if(this.rank == t.rank && this.url == t.url && this.did == t.did && this.position == t.position)
			result = true;
		return result;
	}

	@Override
	public int compareTo(SingleRecordWriteable tmp) {
		return this.rank.get() - tmp.rank.get();
	}
}