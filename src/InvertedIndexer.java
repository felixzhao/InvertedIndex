import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme; 

public class InvertedIndexer {
	//map类
	public static class InvertedIndexerMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
	{
		//	Return Single Value format as:
		//	DID;Rank(Hot);URL;Position
		public void map(LongWritable key,Text val,OutputCollector<Text, Text> output, Reporter reporter
				) throws IOException 
		{
			//	Split Content
			String spl = "%%%%%%";
			String[] str=val.toString().split(spl);		
			if(str[3] != "null")
			{
//				System.out.println("key : "+ str[0]);
//				System.out.println();
//				System.out.println("URL : "+str[1]);
//				System.out.println();
//				System.out.println("Title : "+str[2]);
//				System.out.println();
//				System.out.println("Body : "+str[3]);
//				System.out.println();
//				System.out.println("HOT : "+str[4]);
//				System.out.println();
				
				String br = ";";
				String Content = str[0] + br + str[4] + br + str[1];

				//	Word Break
				// 	Combine Title & Content as a word set to word break 
				StringReader strreader =new StringReader(str[2]+""+str[3]);
				IKSegmenter IK=new IKSegmenter(strreader,true);
				Lexeme lex=null;
				
//				System.out.println("%%value start"+ str[2] + "" +str[3]);
//				System.out.println();
				
				while((lex=IK.next())!=null)
				{	
					
					Text keyWord = new Text(lex.getLexemeText());
					String position = Integer.toString(lex.getBeginPosition());
					Text curValue = new Text(Content + br + position);
					
					System.out.println("key : " + keyWord);
					System.out.println();
					System.out.println("value : " + curValue);
					System.out.println();
					
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
	        
//			String br = " ; ";
//			// git test
//	    	String content = "";
//	        while (values.hasNext()) {
//	        	content += values.next().toString() + br;
//	        }
//	        content += " ###&&### ";
//	        newValue.set(content);
	        
	        newValue.set(values.next().toString());
	        
	        System.out.println("key : "+key);
			System.out.println();
			System.out.println("value : "+newValue);
			System.out.println();
	        
	        output.collect(key, newValue);
		}
	}

	public static void main(String[] args) throws Exception {
	      JobConf conf = new JobConf(InvertedIndexer.class);
	      conf.setJobName("InvertedIndexer");
	
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(Text.class);
	
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


