package Task8;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class Mappers 
extends Mapper<LongWritable,Text,Text,LongWritable> 
{
	private final static String Pattern="|";
	private final static String NA="NA";
	
	  private final static LongWritable one = new LongWritable(1);
	  Text Comp_Name=new Text();
	
	@Override
	public void map(LongWritable key,Text value, Context context)
	throws IOException,InterruptedException
	{
		String line=value.toString();
		
		StringTokenizer tokenizer=new StringTokenizer(line,Pattern);
		
		String Company=tokenizer.nextToken();
		String Prod_Name=tokenizer.nextToken();
		
				
		if(!Company.equalsIgnoreCase(NA) && !Prod_Name.equalsIgnoreCase(NA))
			{
					Comp_Name.set(Company.toString());
					context.write(Comp_Name,one);				
			}
	}
}	
