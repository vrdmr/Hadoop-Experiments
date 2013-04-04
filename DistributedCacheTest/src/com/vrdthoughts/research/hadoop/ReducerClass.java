package com.vrdthoughts.research.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer Class
 *
 * @author Varad Meru, Orzota, Inc.,
 * 		Company - http://www.orzota.com,
 * 		Personal - http://vrdthoughts.com
 *
 * @Category Hadoop Code Snippets
 * @version 0.1
 * @Date 04 Apr 2013
 */
public class ReducerClass extends Reducer<Text, Text, Text, Text> {

	/*
	 * Doing nothing in our example
	 */
	protected void reduce(
			Text arg0,
			java.lang.Iterable<Text> arg1,
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context arg2)
			throws java.io.IOException, InterruptedException {
	};
}