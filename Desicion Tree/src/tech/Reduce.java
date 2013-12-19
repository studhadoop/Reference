package tech;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;

public  class Reduce extends MapReduceBase
implements Reducer<Text, IntWritable, Text, IntWritable> {

	static int cnt =0;
	ArrayList<String> ar = new ArrayList<String>();
	String data = null;
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
		System.out.println("In reducer");
		
		int sum = 0;
		String line = key.toString();
		StringTokenizer itr = new StringTokenizer(line);
		while (values.hasNext()) {
			sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));

		String data = key+" "+sum;
		ar.add(data);

		//	System.out.println("Array List : "+ar.toString()); 

		writeToFile(ar);
		ar.add("\n");
		int index=Integer.parseInt(itr.nextToken());
		String value=itr.nextToken();
		String classLabel=itr.nextToken();
		int count=sum;

	}

	public static void writeToFile(ArrayList<String>  text) {
		System.out.println("In reduce write to file ");
		
		try {

			cnt++;
			C45 id=new C45();
			System.out.println("count "+cnt);

			Path input = new Path("C45/intermediate"+id.current_index+".txt");
			//Path chck = new Path("C45/cnt_"+id.current_index+".txt");
			Configuration conf = new Configuration();
//			conf.set("fs.defaultFS", "my-remote-ip");
//			conf.set("hadoop.job.ugi", "hdfs");

		
			FileSystem fs = FileSystem.get(conf);
			BufferedWriter bw = new BufferedWriter(
					new OutputStreamWriter(fs.create(input, true)));
			
			System.out.println("Text from Reducer: "+ "C45/intermediate"+id.current_index+".txt"+ text);
			System.out.println("file exists:C45/intermediate"+id.current_index+".txt"+fs.exists(input));

			for(String str: text) {
				bw.write(str);
			}

			bw.newLine();
			bw.close();
			
			//	System.out.println("file exists:"+fs.exists(input));



		} catch (Exception e) {
			System.out.println("File is not creating in reduce");
		}
	}

}
