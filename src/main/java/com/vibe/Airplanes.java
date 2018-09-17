package com.vibe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;


public class Airplanes extends Configured implements Tool{
	
	/**
	 * Main function which calls the run method and passes the args using ToolRunner
	 * @param args Two arguments input and output file paths
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Airplanes(), args);
		System.exit(exitCode);
	}
 
	/**
	 * Run method which schedules the Hadoop Job
	 * @param args Arguments passed in main function
	 */
	public int run(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.printf("%s - Se necesita la dirección local del archivo a ser copiado.\n",
					getClass().getSimpleName());
			return -1;
		}

		String localInputPath = args[0];
		Path outputPath = new Path(args[0]);// ARGUMENT FOR OUTPUT_LOCATION
		String[] parts = localInputPath.split("/");
		String part = parts[parts.length-1]; // 004
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://192.168.56.101:54310");
		//cambiar el puerto en caso de que haga falta
		FileSystem fs = FileSystem.get(configuration);
		OutputStream os = fs.create(new Path("/user/hduser/" + part));
		// El path esta definido dentro del filesystem del hdfs, puede ser cualquiera.
		InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));//Data set is getting copied into input stream through buffer mechanism.
		IOUtils.copyBytes(is, os, configuration); // Copying the dataset from input stream to output stream
		System.out.printf("Archivo %s copiado correctamente al hdfs \n",part);
		return 0;

	}
}
