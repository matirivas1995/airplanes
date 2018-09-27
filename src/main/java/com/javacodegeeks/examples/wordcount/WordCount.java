package com.javacodegeeks.examples.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class WordCount extends Configured implements Tool{
    private String pathToHdfs="/user/santiagoarce/";//root path
    Configuration configuration = null;
    List<OutputStream> años = new ArrayList<OutputStream>();
    BufferedWriter br;
	
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
        FileWriter file = null;
        //List<OutputStream> años = new ArrayList<OutputStream>();

        String localDirectoryAuxFile="/Users/santiagoarce/Desktop/wordcount/file.json";

		if (args.length != 1) {
			System.err.printf("%s - Se necesita la dirección local del archivo a ser copiado.\n",
					getClass().getSimpleName());
			return -1;
		}

		String localInputPath = args[0];
		String[] parts = localInputPath.split("/");

        configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(configuration);

        ////
        JSONArray a = (JSONArray) parser.parse(new FileReader(localInputPath));

        int index = 0;
        for (Object o : a)
        {
            JSONObject data = (JSONObject) o;

            JSONObject time = (JSONObject) data.get("time");

            Long year = (Long) time.get("year");


            Path fileHdfs =new Path(pathToHdfs + year);


            if(!fs.exists( fileHdfs)) {
              /*  if (os!= null){
                    os.close();
                }*/
                boolean isCreated = fs.mkdirs(fileHdfs);

                if (isCreated) {
                    System.out.println("Directory created");
                } else {
                    System.out.println("Directory creation failed");
                }
                //años.add(os);

            }else{
                //fs.open(fileHdfs);
                escribirEnCarpeta(data,index++,year,fs);
                //BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
                //br.write(obj.toJSONString());
                //br.close();

            }


        }

        for (OutputStream año:años) {
            año.close();
            System.out.println("Cerrando Carpetas ");
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
	public void escribirEnCarpeta(JSONObject data, int index,Long year,FileSystem fs) throws IOException {

        OutputStream os;

        JSONObject airport = (JSONObject) data.get("airport");

        JSONObject carrier = (JSONObject) data.get("carrier");

        JSONObject statistics = (JSONObject) data.get("statistics");

        JSONObject flights = (JSONObject) statistics.get("flights");

        Long cancelled = (Long) flights.get("cancelled");

        String code = (String) airport.get("code");

        String name = (String) airport.get("name");

        String codeCarrier = (String) carrier.get("code");

        String nameCarrier = (String) carrier.get("name");



        JSONObject obj = new JSONObject();

        obj.put("carrierCode", codeCarrier);
        System.out.println(codeCarrier);
        obj.put("carrierName", nameCarrier);
        System.out.println(code);
        obj.put("ts", new Timestamp(System.currentTimeMillis()).toString());

        Path fileCodeCarrier =new Path(pathToHdfs + year + "/" + codeCarrier);
        if (!fs.exists(fileCodeCarrier)){
            os = fs.create(fileCodeCarrier);
        }else{
            os =fs.append(fileCodeCarrier);
        }

        br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write(obj.toJSONString());
        br.close();




        //aeropuerto
        obj = new JSONObject();
        obj.put("aeroCode", code);
        System.out.println(code);
        obj.put("aeroName", name);
        System.out.println(name);
        obj.put("ts", new Timestamp(System.currentTimeMillis()).toString());

        fileCodeCarrier =new Path(pathToHdfs + year + "/" + code);
        if (!fs.exists(fileCodeCarrier)){
            os = fs.create(fileCodeCarrier);
        }else{
            os =fs.append(fileCodeCarrier);
        }

        br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write(obj.toJSONString());
        br.close();





        //aeropuerto_carrier
        obj = new JSONObject();
        obj.put("aeroCode", code);
        System.out.println(code);
        obj.put("carrierCode", codeCarrier);
        System.out.println(codeCarrier);
        obj.put("ts", new Timestamp(System.currentTimeMillis()).toString());

        fileCodeCarrier =new Path(pathToHdfs + year + "/" + code + "_" +codeCarrier);
        if (!fs.exists(fileCodeCarrier)){
            os = fs.create(fileCodeCarrier);
        }else{
            os =fs.append(fileCodeCarrier);
        }

        br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write(obj.toJSONString());
        br.close();




        //estadisticas
        obj = new JSONObject();
        obj.put("aeroCode", code);
        System.out.println(code);
        obj.put("carrierCode", codeCarrier);
        System.out.println(codeCarrier);
        obj.put("estadisticaCode", index);
        System.out.println(index);
        obj.put("ts", new Timestamp(System.currentTimeMillis()).toString());

        fileCodeCarrier =new Path(pathToHdfs + year + "/estadistica_" + index);
        if (!fs.exists(fileCodeCarrier)){
            os = fs.create(fileCodeCarrier);
        }else{
            os =fs.append(fileCodeCarrier);
        }

        br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write(obj.toJSONString());
        br.close();



        //estadisticas_cancelado

        obj = new JSONObject();
        obj.put("estadisticaCode", index);
        System.out.println(index);
        obj.put("cancelado", cancelled);
        System.out.println(nameCarrier);
        obj.put("ts", new Timestamp(System.currentTimeMillis()).toString());

        fileCodeCarrier =new Path(pathToHdfs + year + "/" + index + "_cancelado");
        if (!fs.exists(fileCodeCarrier)){
            os = fs.create(fileCodeCarrier);
        }else{
            os =fs.append(fileCodeCarrier);
        }

        br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write(obj.toJSONString());
        br.close();

    }
}
