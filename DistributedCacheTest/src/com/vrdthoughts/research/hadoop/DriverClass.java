package com.vrdthoughts.research.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Code Snippet for Distributed Cache. This is the Driver Class for the
 * MapReduce Program to show the working of Distributed Cache.
 *
 * @author Varad Meru, Orzota, Inc.
 * @URL http://vrdthoughts.com
 * @category Hadoop Code Snippets
 * @version 0.1
 * @Date 04 Apr 2013
 */
public class DriverClass extends Configured implements Tool {

	// Main method
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new DriverClass(),
				args);
		// Exits with a a status update.
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		boolean success = false;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		final String NAME_NODE = conf.get("fs.default.name");

		// Absolute path of your cache file.
		final String ABS_PATH_OF_DISTRIBUTED_CACHE_FILE = "/user/varadmeru/cache.dat";

		// Just a precaution if the properties are not set correctly.
		// Should be set with the format hdfs://<namenode>:<port>
		if (NAME_NODE != null) {
			DistributedCache.addCacheFile(new URI(NAME_NODE
					+ ABS_PATH_OF_DISTRIBUTED_CACHE_FILE), conf);
		}

		Job job = new Job(conf, "DistributedCache");
		job.setJarByClass(DriverClass.class);

		// Output data formats for Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Final Output data formats (output of reducer)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Specifying the Mapper and Reducer Class
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);

		// Specifying the paths for input and output dirs
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Submitting the job
		success = job.waitForCompletion(true);

		// Returning the success/failure info back to the calling method (main)
		return success ? 0 : 1;
	}
}