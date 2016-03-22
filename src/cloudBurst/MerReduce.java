package cloudBurst;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
//import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MerReduce {
	
	//------------------------- getStackTrace --------------------------
	
	public static String getStackTrace(Throwable t)
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		t.printStackTrace(pw);
		pw.flush();
		sw.flush();
		return sw.toString();
	}
	
	
	//------------------------- MapClass --------------------------
	
	public static class MapClass extends MapReduceBase implements
			Mapper<IntWritable, BytesWritable, BytesWritable, BytesWritable> 
	{
		private FastaRecord record = new FastaRecord();
		private BytesWritable seed = new BytesWritable();
		private MerRecord seedInfo = new MerRecord();
		
		
		private int MIN_READ_LEN;
		private int MAX_READ_LEN;
		private int SEED_LEN;
		private int FLANK_LEN;
		private int K;
		private int REDUNDANCY;
		private String curfile;
		private String refpath;
		private byte [] seedbuffer = null;
		
		boolean ISREF;

		//------------------------- configure --------------------------
		// Set runtime parameters
		
		public void configure(JobConf conf) 
		{
			curfile = conf.get("map.input.file");
			refpath = conf.get("refpath");
			ISREF = (curfile.indexOf(refpath) != -1);
			
			MIN_READ_LEN = Integer.parseInt(conf.get("MIN_READ_LEN"));
			MAX_READ_LEN = Integer.parseInt(conf.get("MAX_READ_LEN"));
			SEED_LEN     = Integer.parseInt(conf.get("SEED_LEN"));
			FLANK_LEN    = Integer.parseInt(conf.get("FLANK_LEN"));
			K            = Integer.parseInt(conf.get("K"));
			REDUNDANCY   = Integer.parseInt(conf.get("REDUNDANCY"));
			
			seedbuffer   = new byte[DNAString.arrToSeedLen(SEED_LEN, REDUNDANCY)];
		}

		
		//------------------------- map --------------------------
		public void map(IntWritable id, BytesWritable rawRecord,
				        OutputCollector<BytesWritable, BytesWritable> output, 
				        Reporter reporter) throws IOException 
		{
			record.fromBytes(rawRecord);
			
			byte [] seq         = record.m_sequence;
			int realoffsetstart = record.m_offset;
			boolean isLast      = record.m_lastChunk;
			
			seedInfo.id          = id.get();
			seedInfo.isReference = ISREF;
			seedInfo.isRC        = false;
			
			int seqlen = seq.length;
				
			if (ISREF)
			{
				//---------------------- Sequence is a chunk of the reference -----------
				
				int startoffset = 0;

				// If I'm not the first chunk, shift over so there is room for the left flank
				if (realoffsetstart != 0)
				{
					startoffset = CloudBurst.CHUNK_OVERLAP + 1 - FLANK_LEN - SEED_LEN;
					realoffsetstart += startoffset;
				}

				// stop so the last mer will just fit
				int end = seqlen - SEED_LEN + 1;

				// if I'm not the last chunk, stop so the right flank will fit as well
				if (!isLast)
				{
					end -= FLANK_LEN;
				}

				// emit the mers starting at every position in the range
				for (int start = startoffset, realoffset = realoffsetstart; start < end; start++, realoffset++)
				{						
					if (DNAString.arrHasN(seq, start, SEED_LEN)) { continue; } // don't bother with seeds with n's
					
					seedInfo.offset = realoffset;

                    // figure out the ranges for the flanking sequence
					int leftstart = start-FLANK_LEN;
					if (leftstart < 0) { leftstart = 0; }
					int leftlen = start-leftstart;
					
					int rightstart = start+SEED_LEN;
					int rightend = rightstart + FLANK_LEN;
					if (rightend > seqlen) { rightend = seqlen; }
					int rightlen = rightend-rightstart;
					
					BytesWritable seedbinary = seedInfo.toBytes(seq, leftstart, leftlen, rightstart, rightlen);
					
					if ((REDUNDANCY > 1) && (DNAString.repseed(seq, start, SEED_LEN)))
					{
						for (int r = 0; r < REDUNDANCY; r++)
						{
							DNAString.arrToSeed(seq, start, SEED_LEN, seedbuffer, 0, r, REDUNDANCY, 0);
							seed.set(seedbuffer, 0, seedbuffer.length);

							output.collect(seed, seedbinary);
						}
					}
					else
					{
						DNAString.arrToSeed(seq, start, SEED_LEN, seedbuffer, 0, 0, REDUNDANCY, 0);
						seed.set(seedbuffer, 0, seedbuffer.length);

						output.collect(seed, seedbinary);						
					}
				}
			}
			else
			{
				//------------------------ Sequence is a read record -----------------
				
				if (seqlen < MIN_READ_LEN)
				{
					throw new IOException("ERROR: seqlen=" + seqlen + " < MIN_READ_LEN=" + MIN_READ_LEN + " in " + curfile);
				}

				if (seqlen > MAX_READ_LEN)
				{
					throw new IOException("ERROR: seqlen=" + seqlen + " > MAX_READ_LEN=" + MAX_READ_LEN + " in " + curfile + " ref:" + refpath);
				}

				// Skip reads that can't possibly align end-to-end with <= K differences
				// filtering:     55.406s  2583708 map records, 79129 alignments
				// non-filtering: 55.412s, 2584444 map records, 79129 alignments

				int numN = 0;
				for (int i = 0; i < seqlen; i++)
				{
					if (seq[i] == 'N') { numN++; }
				}
				
				if (numN > K) { return; }

				for (int rc = 0; rc < 2; rc++)
				{
					if (rc == 1) 
					{
						// reverse complement the sequence
						DNAString.rcarr_inplace(seq);
						seedInfo.isRC = true;
					}

					// only emit the non-overlapping mers
					for (int i = 0; i + SEED_LEN <= seqlen; i += SEED_LEN)
					{
						if (DNAString.arrHasN(seq, i, SEED_LEN)) { continue; }
						
						if ((REDUNDANCY > 1) && (DNAString.repseed(seq, i, SEED_LEN)))
						{
							DNAString.arrToSeed(seq, i, SEED_LEN, seedbuffer, 0, seedInfo.id, REDUNDANCY, 1);	
						}
						else
						{
							DNAString.arrToSeed(seq, i, SEED_LEN, seedbuffer, 0, 0, REDUNDANCY, 1);
						}
						
						seed.set(seedbuffer, 0, seedbuffer.length);

						seedInfo.offset = i;
						
	                    // figure out the ranges for the flanking sequence
						int leftstart = 0;
						int leftlen = i;
						
						int rightstart = i+SEED_LEN;
						int rightlen = seqlen-rightstart;

						output.collect(seed, seedInfo.toBytes(seq, leftstart, leftlen, rightstart, rightlen));
					}
				}
			}
		}
	}
	
	
	// -- Use a customer partitioner so reference and qry seeds will be grouped together
	public static class PartitionMers implements Partitioner<BytesWritable, BytesWritable>
	{
		private static int seedlen;
		
		public void configure(JobConf conf)
		{
			int SEED_LEN     = Integer.parseInt(conf.get("SEED_LEN"));
			int REDUNDANCY   = Integer.parseInt(conf.get("REDUNDANCY"));
			
			initBuffer(SEED_LEN, REDUNDANCY);
		}
		
		public static void initBuffer(int slen, int redundancy)
		{
			seedlen = slen;
		}
		
		public int getPartition(BytesWritable key, BytesWritable value, int numPartitions)
		{
			// hash over everything except the last byte (ref/qry flag)
			int part = (WritableComparator.hashBytes(key.get(), key.getSize()-1) & Integer.MAX_VALUE) % numPartitions;
			
			//byte [] seedstr = DNAString.seedToArr(key.get(), seedlen, 1);
			//System.out.println("partition: " + DNAString.bytesToString(seedstr) + " " + part);
			//DNAString.printHex("raw", key.get(), 0, key.getSize());
			
			return part;
		}	
	}	
	
	
		
	

//		// -- Use a customer comparator so reference seeds are grouped with qry seeds with the same mer
//		public static class GroupMers implements RawComparator<BytesWritable>
//		{		
//			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
//			{
//				//System.err.println("bcomparing: " + b1.toString() + " and " + b2.toString());
//				
//				int len = l1-1;
//				
//				for (int i = 0; i < len; i++)
//				{
//					int diff = b1[i] - b2[i];
//					if (diff != 0) { return diff; }
//				}
//				
//				return 0;
//			}
//			
//			public int compare(BytesWritable o1, BytesWritable o2) 
//			{
//				return compare(o1.get(), o1.getCapacity(), o1.getSize(), o2.get(), o2.getCapacity(), o2.getSize());
//			}
//		}

		
	public static class GroupMersWC extends WritableComparator
	{
	    public GroupMersWC()
	    {
	    	super(BytesWritable.class);
	    }

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
			//System.err.println("bcomparing: " + b1.toString() + " and " + b2.toString());		
			int len = Math.min(l1, l2) - 1;
			
			for (int i = 0; i < len; i++) 
			{
				int diff = b1[s1+i] - b2[s2+i];
				if (diff != 0) { return diff; }
			}
			
			return 0;
		}
		
		public int compare(WritableComparable wc1, WritableComparable wc2)
		{
			//System.err.println("Compare BytesWritable");
			
			BytesWritable bw1 = (BytesWritable) wc1;
			BytesWritable bw2 = (BytesWritable) wc2;
			
			byte [] b1 = bw1.get();
			byte [] b2 = bw2.get();
			
			// skip the last byte which has the ref/qry flag
			int len = bw1.getSize()-1;
			
			for (int i = 0; i < len; i++)
			{
				int diff = b1[i] - b2[i];
				if (diff != 0) { return diff; }
			}
			return 0;
		}
	}

	
	//------------------------- ReduceClass --------------------------
	public static class ReduceClass extends MapReduceBase implements
			Reducer<BytesWritable, BytesWritable, IntWritable, BytesWritable> 
	{
		private static AlignmentRecord noalignment = new AlignmentRecord(-1, -1, -1, -1, true);
		private static AlignmentRecord fullalignment = new AlignmentRecord();
		private static IntWritable qryid = new IntWritable();
				
		private static int K;
		private static int SEED_LEN;
		private static int BLOCK_SIZE;
		private static int REDUNDANCY;
		private static boolean ALLOW_DIFFERENCES = false;
		private static boolean FILTER_ALIGNMENTS = false;
		
		private static List<MerRecord> reftuples = new ArrayList<MerRecord>();
		private static List<MerRecord> qrytuples = new ArrayList<MerRecord>();
		
		private static AlignmentRecord [] bestalignments;
		private static AlignmentRecord [] secondalignments;
		private static boolean [] recordsecond;
		private static int [] bestk;
		
		//------------------------- configure --------------------------	
		// Get the runtime parameters
		
		public void configure(JobConf conf) 
		{				
			K                 = Integer.parseInt(conf.get("K"));
			SEED_LEN          = Integer.parseInt(conf.get("SEED_LEN"));
			ALLOW_DIFFERENCES = Integer.parseInt(conf.get("ALLOW_DIFFERENCES")) == 1;
			BLOCK_SIZE        = Integer.parseInt(conf.get("BLOCK_SIZE"));
			REDUNDANCY        = Integer.parseInt(conf.get("REDUNDANCY"));
			FILTER_ALIGNMENTS = Integer.parseInt(conf.get("FILTER_ALIGNMENTS")) == 1;
			
			if (FILTER_ALIGNMENTS)
			{
				bestalignments   = new AlignmentRecord[BLOCK_SIZE];
				secondalignments = new AlignmentRecord[BLOCK_SIZE];
				recordsecond     = new boolean[BLOCK_SIZE];
				bestk            = new int[BLOCK_SIZE];
				
				for (int i = 0; i < BLOCK_SIZE; i++)
				{
					bestalignments[i]   = new AlignmentRecord();
					secondalignments[i] = new AlignmentRecord();
				}
			}
			
			LandauVishkin.configure(K);
		}
				
		
		//------------------------- extend --------------------------
		// Given an exact shared seed, try to extend to a full length alignment
		
		public static AlignmentRecord extend(MerRecord qrytuple, MerRecord reftuple,
						                     OutputCollector<IntWritable, BytesWritable> output, 
						                     Reporter reporter) throws IOException 
		{
			int refStart    = reftuple.offset;
			int refEnd      = reftuple.offset + SEED_LEN;
			int differences = 0;
			
			try
			{				
				if (qrytuple.leftFlank.length != 0)
				{
					// at least 1 read base on the left needs to be aligned
					int realleftflanklen = DNAString.dnaArrLen(qrytuple.leftFlank);
					
					// aligned the pre-reversed strings!
					AlignInfo a = cloudBurst.LandauVishkin.extend(reftuple.leftFlank, 
							                                       qrytuple.leftFlank, 
							                                       K, ALLOW_DIFFERENCES);
					
					if (a.alignlen == -1) { return noalignment; } // alignment failed
					if (!a.isBazeaYatesSeed(realleftflanklen, SEED_LEN)) { return noalignment; }
					
					refStart    -= a.alignlen;
					differences = a.differences;
				}
				
				if (qrytuple.rightFlank.length != 0)
				{
					AlignInfo b = cloudBurst.LandauVishkin.extend(reftuple.rightFlank, 
							                                       qrytuple.rightFlank, 
							                                       K - differences, 
							                                       ALLOW_DIFFERENCES);
				
					if (b.alignlen == -1) {	return noalignment;	} // alignment failed
				
					refEnd      += b.alignlen;
					differences += b.differences;
				}

				fullalignment.m_refID       = reftuple.id;
				fullalignment.m_refStart    = refStart;
				fullalignment.m_refEnd      = refEnd;
				fullalignment.m_differences = differences;
				fullalignment.m_isRC        = qrytuple.isRC;
				
				return fullalignment;
			}
			catch (Exception e)
			{
				throw new IOException("Problem with read:" + qrytuple.id + " :" + e.getMessage() + "\n" + getStackTrace(e));	
			}
		}
		
		
		//------------------------- reduce --------------------------
		public synchronized void reduce(BytesWritable mer, Iterator<BytesWritable> values,
										OutputCollector<IntWritable, BytesWritable> output, Reporter reporter)
										throws IOException 
		{
			Timer timer = new Timer();
			
			reftuples.clear();
			qrytuples.clear();

			final boolean verbose = false;
			String seedstr = "";
			
			if (verbose)
			{
				seedstr = DNAString.bytesToString(DNAString.seedToArr(mer.get(), SEED_LEN, REDUNDANCY));
				System.err.println("Working on: " + seedstr);
			}
			
			MerRecord merIn;
			
			int totalr = 0;
			int totalq = 0;
			int qbatch = 0;
			
			// Reference mers are first, save them away
			while (values.hasNext()) 
			{
				merIn = new MerRecord(values.next());
				
				if (verbose)
				{
				  System.err.println("  Got: " + merIn.toString());
				}

				if (merIn.isReference) 
				{
					// just save away the reference tuples
					totalr++;
					reftuples.add(merIn);
					
					if (totalq != 0)
					{
						String ss = DNAString.bytesToString(DNAString.seedToArr(mer.get(), SEED_LEN, REDUNDANCY));
						throw new IOException("ERROR: Saw a reference seed after a query seed for: " + ss);
					}
				}	
				else                   
				{
					if (totalr == 0)
					{
						// got a qry tuple, but there were no reference tuples
						//System.err.println(" Saw a query tuple, but no referernce tuple!!!");
						
						return;
					}

					qrytuples.add(merIn);
					totalq++;
					qbatch++;
					
					if (qbatch == BLOCK_SIZE)
					{
						alignBatch(output, reporter);
					
						qrytuples.clear();
						qbatch = 0;
					}
				}
			}
			
			if (qbatch != 0)
			{
				alignBatch(output, reporter);
			}
			
			if (verbose)
			{
				reporter.setStatus(seedstr + " : " + totalr + " x " + totalq + " = " + totalr*totalq + " " + timer.get());
				System.err.println(seedstr + " : " + totalr + " x " + totalq + " = " + totalr*totalq + " " + timer.get());
			}
		}
		
		public static void alignBatch(OutputCollector<IntWritable, BytesWritable> output, Reporter reporter) throws IOException
		{
			int numr = reftuples.size();
			int numq = qrytuples.size();

			// join together the query-ref shared mers
			if ((numr != 0) && (numq != 0))
			{		
				// Align reads to the references in blocks of BLOCK_SIZE x BLOCK_SIZE to improve cache locality
				// define a qry block between [startq, lastq)
				for (int startq = 0; startq < numq; startq += BLOCK_SIZE)
				{
					int lastq = startq + BLOCK_SIZE;
					if (lastq > numq) { lastq = numq; }
					
					if (FILTER_ALIGNMENTS)
					{
					  java.util.Arrays.fill(bestk, K+1);
					}
					
					// define a ref block between [startr, lastr)
					for (int startr = 0; startr < numr; startr += BLOCK_SIZE)
					{
						int lastr = startr + BLOCK_SIZE;
						if (lastr > numr) { lastr = numr; }

						// for each element in [startq, lastq)
						for (int curq = startq; curq < lastq; curq++)
						{
							MerRecord qry = qrytuples.get(curq);
							
							// for each element in [startr, lastr)
							for (int curr = startr; curr < lastr; curr++)
							{
								AlignmentRecord rec = extend(qry, reftuples.get(curr), output, reporter);
								
								if (rec.m_differences == -1) continue;
								
								if (FILTER_ALIGNMENTS)
								{
									int qidx = curq - startq;
									if (rec.m_differences < bestk[qidx])
									{ 
										bestk[qidx] = rec.m_differences;
										bestalignments[qidx].set(rec);
										recordsecond[qidx] = false;
									}
									else if (rec.m_differences == bestk[qidx])
									{	
										secondalignments[qidx].set(rec);
										recordsecond[qidx] = true;
									}
								}
								else
								{
									qryid.set(qry.id);
									output.collect(qryid, fullalignment.toBytes());
								}
							}
						}
					}
					
					if (FILTER_ALIGNMENTS)
					{
						for (int qidx = 0; qidx < lastq - startq; qidx++)
						{
							if (bestk[qidx] <= K)
							{
								qryid.set(qrytuples.get(qidx+startq).id);
								output.collect(qryid, bestalignments[qidx].toBytes());
								
								if (recordsecond[qidx])
								{
									output.collect(qryid, secondalignments[qidx].toBytes());
								}
							}
						}
					}
				}
			}

		}
	}
}
