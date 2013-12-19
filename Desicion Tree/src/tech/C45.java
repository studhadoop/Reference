package tech;

import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Algorithm:
CurrentNode is assumed for splitting.
Map(key, value)
{


Checks whether this instance belongs to CurrentNode or not.
For all uncovered attributes it outputs index and its value
and class label of instance.
}
Reduce(key, value)
{
counts number of occurrences of combination of ( index and
its value and class Label ) and prints count against it.
}
We calculate the Gain Ratio from the data available from
reduce function.
All the child (split) nodes that are made from parent node
are pushed on to queue.
Every Node is represented by a list of attribute indexes and
its values.
While(CurrentNode is not last Node in Queue)
if(Entropy!=0 we have some more uncovered attributes for
splitting)

 * */

public class C45 extends Configured implements Tool {


	public static Split currentsplit=new Split();

	public static List <Split> splitted=new ArrayList<Split>();;

	public static int current_index=0;
	public static ArrayList<String> ar = new ArrayList<String>();
	public static void main(String[] args) throws Exception {

		//System.out.println("In main");
		MapClass mp=new MapClass();
		splitted.add(currentsplit);
		String myarg = args[1];


		Path c45 = new Path("C45");
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "my remote -ip");
//		conf.set("hadoop.job.ugi", "hdfs");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(c45)){
			fs.delete(c45, true);
		}
		fs.mkdirs(c45);

		int res=0;
		double bestGain=0;
		boolean stop=true;
		boolean outerStop=true;
		int split_index=0;
		double gainratio=0;
		double best_gainratio=0;
		double entropy=0;
		String classLabel=null;
		int total_attributes=mp.no_Attr;
		total_attributes=4;
		int split_size=splitted.size();
		GainRatio gainObj;
		Split newnode;

		Reduce r = new Reduce();
		while(split_size>current_index)
		{
			System.out.println("In main ---- while loop");
			currentsplit=(Split) splitted.get(current_index); 
			gainObj=new GainRatio();

			res = ToolRunner.run(new Configuration(), new C45(), args);

			System.out.println("Current  NODE INDEX . ::"+current_index);


			int j=0;
			int temp_size;
			gainObj.getcount();
			entropy=gainObj.currNodeEntophy();
			classLabel=gainObj.majorityLabel();
			currentsplit.classLabel=classLabel;



			if(entropy!=0.0 && currentsplit.attr_index.size()!=total_attributes)
			{
				System.out.println("");
				System.out.println("Entropy  NOTT zero   SPLIT INDEX::    "+entropy);

				best_gainratio=0;

				for(j=0;j<total_attributes;j++)		//Finding the gain of each attribute
				{

					if(currentsplit.attr_index.contains(j))  // Splitting all ready done with this attribute
					{
						// System.out.println("Splitting all ready done with  index  "+j);
					}
					else
					{
						gainratio=gainObj.gainratio(j,entropy);

						if(gainratio>=best_gainratio)
						{
							split_index=j;

							best_gainratio=gainratio;
						}

					}
				}

				String attr_values_split=gainObj.getvalues(split_index);
				StringTokenizer attrs = new StringTokenizer(attr_values_split);
				int number_splits=attrs.countTokens(); //number of splits possible with  attribute selected
				String red="";
				int tred=-1;



				System.out.println(" INDEX ::  "+split_index);
				System.out.println(" SPLITTING VALUES  "+attr_values_split);

				for(int splitnumber=1;splitnumber<=number_splits;splitnumber++)
				{



					temp_size=currentsplit.attr_index.size();
					newnode=new Split(); 
					for(int y=0;y<temp_size;y++)   // CLONING OBJECT CURRENT NODE
					{

						newnode.attr_index.add(currentsplit.attr_index.get(y));
						newnode.attr_value.add(currentsplit.attr_value.get(y));
					}
					red=attrs.nextToken();

					newnode.attr_index.add(split_index);
					newnode.attr_value.add(red);
					splitted.add(newnode);
				}
			}
			else
			{
				System.out.println("");
				String rule="";
				temp_size=currentsplit.attr_index.size();
				for(int val=0;val<temp_size;val++)  
				{
					rule=rule+" "+currentsplit.attr_index.get(val)+" "+currentsplit.attr_value.get(val);
				}
				rule=rule+" "+currentsplit.classLabel;
				System.out.println("RULE :"+rule);
				/*
				 * ar.add(data);

	//	System.out.println("Array List : "+ar.toString()); 

		writeToFile(ar);
		ar.add("\n");
				 */
				ar.add(rule);
				writeRuleToFile(ar);
				ar.add("\n");
				if(entropy!=0.0)
					System.out.println("Enter rule in file:: "+rule);
				else
					System.out.println("Enter rule in file Entropy zero ::   "+rule);


			}

			split_size=splitted.size();
			System.out.println("TOTAL NODES::    "+split_size);



			current_index++;

		}
		System.out.println("In main outside while loop");
		System.out.println("COMPLEEEEEEEETEEEEEEEEEE ");
		System.exit(res);

	}

	public static void writeRuleToFile(ArrayList<String>  text) throws IOException {
		
		System.out.println("In main ---- Rule file");
		Path rule = new Path("C45/rule.txt");
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "my-remote-ip");
//		conf.set("hadoop.job.ugi", "hdfs");
		FileSystem fs = FileSystem.get(conf);
		try {


			BufferedWriter bw = new BufferedWriter(
					new OutputStreamWriter(fs.create(rule, true)));
			   
			for(String str: text) {
				bw.write(str);
			}
			
			bw.newLine();
			bw.close();
		} catch (Exception e) {
		}

	}



	public int run(String[] args) throws Exception {
		System.out.println("In main ---- run");
		JobConf conf = new JobConf(getConf(),C45.class);
		conf.setJobName("C45");

		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		System.out.println("back to run");

		//set your input file path below
		//"/home/sreeveni/myfiles/DT/Id3_hds/iris.txt"
		//out - /home/sreeveni/myfiles/DT/DTOut
		FileSystem fs = FileSystem.get(conf);

		Path out = new Path(args[1] + current_index);
		if (fs.exists(out))
			fs.delete(out, true);

		FileInputFormat.setInputPaths(conf,args[0] );
		FileOutputFormat.setOutputPath(conf,out);

		JobClient.runJob(conf);
		return 0;
	}



}
