package com.javacodegeeks.examples.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;

public class WordCount extends Configured implements Tool{
	
	/**
	 * Main function which calls the run method and passes the args using ToolRunner
	 * @param args Two arguments input and output file paths
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}
 
	/**
	 * Run method which schedules the Hadoop Job
	 * @param args Arguments passed in main function
	 */
	public int run(String[] args) throws Exception {
        JSONParser parser = new JSONParser();
        String namePasado = null;
        OutputStream os = null;
        FileWriter file = null;
        String pathToHdfs="/user/santiagoarce/";//root path
        String localDirectoryAuxFile="/Users/santiagoarce/Desktop/wordcount/file.json";
        Configuration configuration = null;
	/*	Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem filesystem = FileSystem.get(configuration);
		FileUtil.copy(filesystem, new Path("prueba.txt"), filesystem, new Path("."), false, configuration);*/
		if (args.length != 1) {
			System.err.printf("%s - Se necesita la dirección local del archivo a ser copiado.\n",
					getClass().getSimpleName());
			return -1;
		}

		String localInputPath = args[0];
		Path outputPath = new Path(args[0]);// ARGUMENT FOR OUTPUT_LOCATION
		String[] parts = localInputPath.split("/");
		String part = parts[parts.length-1]; // 004

        ////
        JSONArray a = (JSONArray) parser.parse(new FileReader(localInputPath));
        int index = 0;

        for (Object o : a)
        {

            JSONObject data = (JSONObject) o;

            JSONObject airport = (JSONObject) data.get("airport");

            JSONObject carrier = (JSONObject) data.get("carrier");

            String code = (String) airport.get("code");

            String name = (String) airport.get("name");

            String codeCarrier = (String) carrier.get("code");

            String nameCarrier = (String) carrier.get("name");

            JSONObject obj = new JSONObject();
            obj.put("id", index);
            System.out.println(index);
            obj.put("carrier", nameCarrier);
            System.out.println(nameCarrier);
            obj.put("airport", name);
            System.out.println(name);
            index++;

            if (namePasado==null){
                file = new FileWriter(localDirectoryAuxFile);
                file.write(obj.toJSONString());
                configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem fs = FileSystem.get(configuration);
                os = fs.create(new Path(pathToHdfs + codeCarrier));
                namePasado = codeCarrier;

            }else if (!namePasado.equals(codeCarrier) || index == a.size()){
                file.close();
                InputStream is = new BufferedInputStream(new FileInputStream(localDirectoryAuxFile));
                IOUtils.copyBytes(is, os, configuration); // Copying the dataset from input stream to output stream
                System.out.println("Se copio el contenido de " + namePasado + " al hdfs.");
                System.out.println("\nJSON Object: " + obj);
                configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem fs = FileSystem.get(configuration);

                //if (!fs.exists(new Path("/user/santiagoarce/" + codeCarrier))){
                    os = fs.create(new Path(pathToHdfs + codeCarrier));
                //}
                file = new FileWriter(localDirectoryAuxFile);
                file.write("");
                file.write(obj.toJSONString());
                namePasado = codeCarrier;
            }else{

                file.write(obj.toJSONString());
            }
            //os.write(obj.toJSONString().getBytes());
        }
        /////

/*		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(configuration);
		OutputStream os = fs.create(new Path("/user/santiagoarce/" + part));*/
		//"/Users/santiagoarce/Desktop/wordcount/prueba.txt"



		//InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));//Data set is getting copied into input stream through buffer mechanism.
		//IOUtils.copyBytes(is, os, configuration); // Copying the dataset from input stream to output stream
		//System.out.printf("Archivo %s copiado correctamente al hdfs \n",part);
        if (file != null) {
            file.close();
        }
        return 0;

/*		if (args.length != 2) {
			System.err.printf("Usage: %s needs two arguments <input> <output> files\n",
					getClass().getSimpleName());
			return -1;
		}
	
		//Initialize the Hadoop job and set the jar as well as the name of the Job
		Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCounter");
		
		//Add input and output file paths to job based on the arguments passed
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the MapClass and ReduceClass in the job
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
	
		//Wait for the job to complete and print if the job was successful or not
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}
		
		return returnValue;*/
	}
}
