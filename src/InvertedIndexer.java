import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.wltea.analyzer.core.*;

public class InvertedIndexer {
	//map类
	public static class InvertedIndexerMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
	{
		//	Return Single Value format as:
		//	DID;Rank(Hot);URL;Position
		public void map(Text key,Text val,OutputCollector<Text, Text> output, Reporter reporter
				) throws IOException 
		{
			//	Split Content
			String spl = "%%%%%%";
			String[] str=val.toString().split(spl);		
			if(str[3] != "null")
			{
				String br = ";";
				String Content = str[0] + br + str[4] + br + str[1];

				//	Word Break
				// 	Combine Title & Content as a word set to word break 
				StringReader strreader =new StringReader(str[2]+""+str[3]);
				IKSegmenter IK=new IKSegmenter(strreader,true);
				Lexeme lex=null;
				
				while((lex=IK.next())!=null)
				{				
					Text keyWord = new Text(lex.getLexemeText());
					String position = Integer.toString(lex.getBeginPosition());
					Text curValue = new Text(Content + br + position);
					
					output.collect(keyWord, curValue);
				}
			}
		}
	}

	//reduce类接收map传来的键值对，生成倒排索引形式的键值对
	//单词(key)	SingleInfo:SingleInfo:…(value) 
	public static class InvertedIndexerReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
	{
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			// 对于接收到的键值对，key为分词后的每个单词，value包含了一组
			// SingleRecordWritable，对于URL相同的SingleRecordWritable把他
			// 们的词频相加，不同的则组合在一起，形成上述的值的形式
			Text newValue = new Text();
	        
			String br = ":";
			// git test
	    	String content = "";
	        while (values.hasNext()) {
	        	content += values.next().toString() + br;
	        }
	        newValue.set(content);
	        
	        output.collect(key, newValue);
		}
	}

	public static void main(String[] args) throws Exception {
	      JobConf conf = new JobConf(InvertedIndexer.class);
	      conf.setJobName("InvertedIndexer");
	
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(IntWritable.class);
	
	      conf.setMapperClass(InvertedIndexerMapper.class);
//	      conf.setCombinerClass(Combin.class);
	      conf.setReducerClass(InvertedIndexerReducer.class);
	
	      conf.setInputFormat(TextInputFormat.class);
	      conf.setOutputFormat(TextOutputFormat.class);
	
	      FileInputFormat.setInputPaths(conf, new Path(args[0]));
	      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	      JobClient.runJob(conf);
	    }

}


