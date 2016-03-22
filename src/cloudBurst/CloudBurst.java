package cloudBurst;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import cloudBurst.MerReduce.MapClass;
import cloudBurst.MerReduce.ReduceClass;
import cloudBurst.FilterAlignments.FilterCombinerClass;
import cloudBurst.FilterAlignments.FilterMapClass;
import cloudBurst.FilterAlignments.FilterReduceClass;

public class CloudBurst {	
	
	// Make sure this number is longer than the longest read
	public static final int CHUNK_OVERLAP = 1024;
	
	
	//------------------------- alignall --------------------------
	// Setup and run the hadoop job for running the alignment

	public static RunningJob alignall(String refpath, 
			                          String qrypath,
			                          String outpath,
			                          int MIN_READ_LEN,
			                          int MAX_READ_LEN,
			                          int K,
			                          int ALLOW_DIFFERENCES,
			                          boolean FILTER_ALIGNMENTS,
			                          int NUM_MAP_TASKS,
			                          int NUM_REDUCE_TASKS,
			                          int BLOCK_SIZE,
			                          int REDUNDANCY) throws IOException, Exception
	{
		int SEED_LEN   = MIN_READ_LEN / (K+1);
		int FLANK_LEN  = MAX_READ_LEN-SEED_LEN+K; 
		
		
		System.out.println("refath: "            + refpath);
		System.out.println("qrypath: "           + qrypath);
		System.out.println("outpath: "           + outpath);
		System.out.println("MIN_READ_LEN: "      + MIN_READ_LEN);
		System.out.println("MAX_READ_LEN: "      + MAX_READ_LEN);
		System.out.println("K: "                 + K);
		System.out.println("SEED_LEN: "          + SEED_LEN);
		System.out.println("FLANK_LEN: "         + FLANK_LEN);
		System.out.println("ALLOW_DIFFERENCES: " + ALLOW_DIFFERENCES);
		System.out.println("FILTER_ALIGNMENTS: " + FILTER_ALIGNMENTS);
		System.out.println("NUM_MAP_TASKS: "     + NUM_MAP_TASKS);
		System.out.println("NUM_REDUCE_TASKS: "  + NUM_REDUCE_TASKS);
		System.out.println("BLOCK_SIZE: "        + BLOCK_SIZE);
		System.out.println("REDUNDANCY: "        + REDUNDANCY);
		
		JobConf conf = new JobConf(MerReduce.class);
		conf.setJobName("CloudBurst");
		conf.setNumMapTasks(NUM_MAP_TASKS);
		conf.setNumReduceTasks(NUM_REDUCE_TASKS);

		// old style
		//conf.addInputPath(new Path(refpath));
		//conf.addInputPath(new Path(qrypath));
		
		// new style
		FileInputFormat.addInputPath(conf, new Path(refpath));
		FileInputFormat.addInputPath(conf, new Path(qrypath));

		conf.set("refpath",           refpath);
		conf.set("qrypath",           qrypath);
		conf.set("MIN_READ_LEN",      Integer.toString(MIN_READ_LEN));
		conf.set("MAX_READ_LEN",      Integer.toString(MAX_READ_LEN));
		conf.set("K",                 Integer.toString(K));
		conf.set("SEED_LEN",          Integer.toString(SEED_LEN));
		conf.set("FLANK_LEN",         Integer.toString(FLANK_LEN));
		conf.set("ALLOW_DIFFERENCES", Integer.toString(ALLOW_DIFFERENCES));
		conf.set("BLOCK_SIZE",        Integer.toString(BLOCK_SIZE));
		conf.set("REDUNDANCY",        Integer.toString(REDUNDANCY));
		conf.set("FILTER_ALIGNMENTS", (FILTER_ALIGNMENTS ? "1" : "0"));
		
		conf.setMapperClass(MapClass.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);			
		conf.setMapOutputKeyClass(BytesWritable.class);
		conf.setMapOutputValueClass(BytesWritable.class);

		// The order of seeds is not important, but make sure the reference seeds are seen before the qry seeds
		conf.setPartitionerClass(MerReduce.PartitionMers.class); 
		conf.setOutputValueGroupingComparator(MerReduce.GroupMersWC.class);
		
		conf.setReducerClass(ReduceClass.class);		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(BytesWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		
		Path oPath = new Path(outpath);
		//conf.setOutputPath(oPath);
		FileOutputFormat.setOutputPath(conf, oPath);
		System.err.println("  Removing old results");
		FileSystem.get(conf).delete(oPath);		
		
		
		RunningJob rj = JobClient.runJob(conf);
		System.err.println("CloudBurst Finished");
		return rj;
	}
	
	
	//------------------------- filter --------------------------
	// Setup and run the hadoop job for filtering the alignments to just report unambiguous bests
	
	public static void filter(String alignpath, 
			                  String outpath,
                              int nummappers,
                              int numreducers) throws IOException, Exception
    {
		System.out.println("NUM_FMAP_TASKS: "     + nummappers);
		System.out.println("NUM_FREDUCE_TASKS: "  + numreducers);
		
		JobConf conf = new JobConf(FilterAlignments.class);
		conf.setJobName("FilterAlignments");
		conf.setNumMapTasks(nummappers);
		conf.setNumReduceTasks(numreducers);
		
		// old style
		//conf.addInputPath(new Path(alignpath));
		FileInputFormat.addInputPath(conf, new Path(alignpath));
		
		conf.setMapperClass(FilterMapClass.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);			
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(BytesWritable.class);
		
		conf.setCombinerClass(FilterCombinerClass.class);
		
		conf.setReducerClass(FilterReduceClass.class);		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(BytesWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		Path oPath = new Path(outpath);
		FileOutputFormat.setOutputPath(conf, oPath);
		//conf.setOutputPath(oPath);
		System.err.println("  Removing old results");
		FileSystem.get(conf).delete(oPath);
		
		JobClient.runJob(conf);
		
		System.err.println("FilterAlignments Finished");		
    }

	
	//------------------------- main --------------------------
	// Parse the command line options, run alignment and filtering
	
	public static void main(String[] args) throws Exception 
	{	
		String refpath = null;
		String qrypath = null;
		String outpath = null;
		
		int K                = 0;
		int minreadlen       = 0;
		int maxreadlen       = 0;
		int allowdifferences = 0;
		
		int nummappers   = 1;
		int numreducers  = 1;
		int numfmappers  = 1;
		int numfreducers = 1;
		int blocksize    = 128;
		int redundancy   = 1;
		
		boolean filteralignments = false;
		
		int local = 1; // set to zero to use command line arguments
		
		if (local == 1)
		{
			/*
				refpath = "/users/mschatz/cloudburst/in/s_suis.br";
				qrypath = "/users/mschatz/cloudburst/in/100k.br";
				outpath = "/users/mschatz/cloudburst/results";
			*/
			refpath = "D:/Workspace/CloudBurst_data/s_suis.br";
			qrypath = "D:/Workspace/CloudBurst_data/100k.br";
			outpath = "D:/Workspace/CloudBurst_data/results";
			
			minreadlen = 36;
			maxreadlen = 36;
			
			K = 2;
			allowdifferences = 0;
			filteralignments = false;
			redundancy       = 1;
		}
		else if (local == 2)
		{
			refpath = "/user/guest/cloudburst/short.ref.br";
			qrypath = "/user/guest/cloudburst/short.qry.br";
			outpath = "/user/guest/br-results";
			minreadlen = 32;
			maxreadlen = 32;
			
			K = 2;
			allowdifferences = 1;
			filteralignments = false;
			redundancy       = 1;
		}
		else if (args.length != 14)
		{
			System.err.println("Usage: CloudBurst refpath qrypath outpath minreadlen maxreadlen k allowdifferences filteralignments #mappers #reduces #fmappers #freducers blocksize redundancy");
  
//			System.err.println();
//			System.err.println("1.  refpath:          path in hdfs to the reference file");
//			System.err.println("2.  qrypath:          path in hdfs to the query file");
//			System.err.println("3.  outpath:          path to a directory to store the results (old results are automatically deleted)");
//			System.err.println("4.  minreadlen:       minimum length of the reads");
//			System.err.println("5.  maxreadlen:       maximum read length");
//			System.err.println("6.  k:                number of mismatches / differences to allow (higher number requires more time)");
//			System.err.println("7.  allowdifferences: 0: mismatches only, 1: indels as well"); 
//			System.err.println("8.  filteralignments: 0: all alignments,  1: only report unambiguous best alignment (results identical to RMAP)");
//			System.err.println("9.  #mappers:         number of mappers to use.              suggested: #processor-cores * 10");
//			System.err.println("10. #reduces:         number of reducers to use.             suggested: #processor-cores * 2");
			System.err.println("11. #fmappers:        number of mappers for filtration alg.  suggested: #processor-cores");
			System.err.println("12. #freducers:       number of reducers for filtration alg. suggested: #processor-cores");
			System.err.println("13. blocksize:        number of qry and ref tuples to consider at a time in the reduce phase. suggested: 128"); 
			System.err.println("14. redundancy:       number of copies of low complexity seeds to use. suggested: # processor cores");
			
			return;
		}
		else
		{
			refpath          = args[0];
			qrypath          = args[1];
			outpath          = args[2];
			minreadlen       = Integer.parseInt(args[3]);
			maxreadlen       = Integer.parseInt(args[4]);
			K                = Integer.parseInt(args[5]);
			allowdifferences = Integer.parseInt(args[6]);
			filteralignments = Integer.parseInt(args[7]) == 1;
			nummappers       = Integer.parseInt(args[8]);
			numreducers      = Integer.parseInt(args[9]);
			numfmappers      = Integer.parseInt(args[10]);
			numfreducers     = Integer.parseInt(args[11]);
			blocksize        = Integer.parseInt(args[12]);
			redundancy       = Integer.parseInt(args[13]);
		}
		
		if (redundancy < 1) { System.err.println("minimum redundancy is 1"); return; }
		
		if (maxreadlen > CHUNK_OVERLAP)
		{
			System.err.println("Increase CHUNK_OVERLAP for " + maxreadlen + " length reads, and reconvert fasta file");
			return;
		}
			
		// start the timer
		Timer all = new Timer();
		
		String alignpath = outpath;
		if (filteralignments) { alignpath += "-alignments"; }
		
		
		// run the alignments
		Timer talign = new Timer();
		alignall(refpath,  qrypath, alignpath, minreadlen, maxreadlen, K, allowdifferences, filteralignments, 
				 nummappers, numreducers, blocksize, redundancy);
		System.err.println("Alignment time: " + talign.get());
		
		// filter to report best alignments
		if (filteralignments)
		{
			Timer tfilter = new Timer();
			filter(alignpath, outpath, numfmappers, numfreducers);
		
			System.err.println("Filtering time: " + tfilter.get());
		}
		
		System.err.println("Total Running time:  " + all.get());
	};
}
