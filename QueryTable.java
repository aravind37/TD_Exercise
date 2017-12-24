import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJob.Status;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.io.*;
import org.apache.commons.cli.*;


public class QueryTable {
	static String format = "TABULAR";
	static String columns = "*";
	static String limit = null;
	static String mintime = null;
	static String maxTime = null;
	static String engine = "presto";
	static String database = null;
	static String table = null;
	static TDClient client;
	
	 public static void main(String[] args) throws InterruptedException
	    {
		 	Options commandLineOptions= buildOptions();
			setOptions(commandLineOptions,args);
			
			//establish a new TDclient connection
			client=TDClient.newClient();
			
			//validate if the the DB name and table name are valid 
			validateDatabase(client);
			validateTable(client);
			
		 	//format the query that we will run based on the parameters provided in the command line
		 	String query = formatQuery();
		 	
			System.out.println("going to run the query " + query);
			
			
		 	String jobId = client.submit(TDJobRequest.newHiveQuery(database, query));
		 	
		 	//Decide the format of report based on users selection criteria
		 	TDResultFormat reportFormat;

            if(format.equalsIgnoreCase("csv"))
            {
            	reportFormat = TDResultFormat.CSV;
            }
            else
            {
            	reportFormat = TDResultFormat.TSV;
            }
            
		 	TDJobSummary job = client.jobStatus(jobId);
		 	ExponentialBackOff backOff = new ExponentialBackOff();
	            while (!job.getStatus().isFinished()) {
	            	System.out.println("Query is executing");
	                Thread.sleep(backOff.nextWaitTimeMillis());
	                job = client.jobStatus(jobId);
	            }
	            
	            // Read the detailed job information
	            TDJob jobInfo = client.jobInfo(jobId);
	            
	            //get the jobStatus
	            Status status = job.getStatus();
	            //set the output format
	            
	            if(status == job.getStatus().SUCCESS)
	            {
	            	if(format.equalsIgnoreCase("tabular"))
        			{
        				printTabularOutput(jobId,reportFormat);
        			}
	            	else 
	            	{
	            		saveToCSV(jobId,reportFormat);
	            	}
	            }
	            else
	            {
	            	System.out.println("error log:\n" + jobInfo.getStdErr());
	            	System.exit(10);
	            }
	            
	    } 
	 
	 private static void printTabularOutput(String jobId,TDResultFormat resultFormat)
	 {
		 client.jobResult(jobId, resultFormat,new Function<InputStream, Boolean>()
 		{
 			
 			public Boolean apply(InputStream input)
 			{
 				try {
 					String resultString = new String(ByteStreams.toByteArray(input));
 					//verify if the query has returned any output
 					if(resultString.length() > 0)
 					{
 						System.out.println(resultString);
							System.out.println("Exiting....");
							
 					
 					}
 					else
 					{
 						System.out.println("Sorry, the query did not return any result");
 						
 						
 					}
 					client.close();
 					System.exit(0);
 					
 					
 				}
 				catch (Exception e)
 				{
 					System.out.println(e.getStackTrace());
 				}
						
 				finally
 				{
 					client.close();
 				}
 				return true;
 				
 			}
 		});
     }
	 
	 private static void saveToCSV(String jobId,TDResultFormat resultFormat)
	 {
		 client.jobResult(jobId, resultFormat,new Function<InputStream, Boolean>()
 		{
 			@Override
 			public Boolean apply(InputStream input)
 			{
 				try {
 					byte[] buffer = ByteStreams.toByteArray(input);
 					if(buffer.length > 0)
 					{
 						File targetFile = new File("Query_Result.csv");
 						System.out.println("The CSV file the name Query_Result.csv is being saved at the location:" +targetFile.getCanonicalPath());
 	 				    Files.write(buffer, targetFile);
 	 				    
 	 				    
 					}
 					else
 					{
 						System.out.println("Sorry the query did not return any results");
 						
 					}
 						
 						client.close();
 						System.exit(0);
 						
						
					} 
 					catch (IOException e) {
						// Catch and print exception related to files
						e.printStackTrace();
						System.exit(1);
					}
 				finally{
 					client.close();
 				}
	 				
 				return true;
 				
 			}
 		});
     	
     }
	     
	 
	 private static void validateDatabase(TDClient client)
	 {
		 
		if(client.existsDatabase(database))
		{
			;
		}
		else
		{
			client.close();
			System.out.println("Please provide a valid Database name. The Database base "+database+" does not exist");
			System.out.println("Exiting with code 8");
			System.exit(8);
		}
	 }
	 private static void validateTable(TDClient client)
	 {
		if(client.existsTable(database, table))
		{
			;
		}
		else
		{
			client.close();
			System.out.println("Please provide a valid table name. The table  "+table+" does not exist in the Database " +database);
			
			System.exit(9);
		}
	 }
	 private static Options buildOptions()
	 {
		 Options option = new Options();
		 option.addOption("f", "format",true, "Format of the output : enter either CSV or Tabular");
		 option.addOption("c", "columns",true, "Provide the list of the columns that you want to query");
		 option.addOption("l", "limit",true, "Limit to output to few rows");
		 option.addOption("m","min" ,true,"Enter the minimum time stamp");
		 option.addOption("M","MAX",true, "Enter the maximum time stap");
		 option.addOption("e", "enginge",true,"Provide the engine type, you can choose between hive or presto");
		 
		 return option;
		 
	 }
	 static void setOptions(Options options, String args[])
	 {
	 		CommandLineParser parser = new DefaultParser();
	 		CommandLine cmd = null;
	 		HelpFormatter formatter = new HelpFormatter();
	 		
			try {
				cmd = parser.parse( options, args);
				//
				//System.out.println("The output value is " +checkFormat);
		 		if(cmd.getOptionValue('f')!= null)
		 		{
		 			setFormatType(cmd);
		 			
		 		}
		 		
		 		if(cmd.getOptionValue('c') != null)
		 		{
		 			setColumns(cmd);
		 		}
		 		
		 		if(cmd.getOptionValue('l')!= null)
		 		{
		 			setLimit(cmd);
		 		}
		 		
		 		if(cmd.getOptionValue('m') != null)
		 		{
		 			setTime(cmd,0);
		 			
		 		}
		 		
		 		if(cmd.getOptionValue('M')!= null)
		 		{
		 			setTime(cmd,1);
	 			}
		 		verifyTime();
		 		
		 		if(cmd.getOptionValue('e') != null)
		 		{
		 			setEngine(cmd);		
	 			}
		 		Object[] getDBTable= cmd.getArgList().toArray();
		 		if(getDBTable.length <2 || getDBTable.length >2)
		 		{
		 			System.out.println("Please provide two required arguments, the DB name and the Table name");
		 			System.exit(1);
		 		}
		 		
		 		database = getDBTable[0].toString();
		 		table= getDBTable[1].toString();
		 		
		 		
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				formatter.printHelp("java jar QueryTable.jar <yourDB> <yourTable> (optional argumets with flags as listed below)", options);
				
				System.exit(2);
			}
	 		
	 	}
	 	private static void setFormatType(CommandLine cmd)
	 	{
	 		String checkFormat = cmd.getOptionValue('f').toString();
 			if(checkFormat.equalsIgnoreCase("tabular") || checkFormat.equalsIgnoreCase("CSV"))
 			{
 				format = checkFormat;
 			}
 			else
 			{
 				System.out.println("Please provide Tabular or CSV as an option for Format");
 				System.exit(5);
 			}
	 	}
	    
	 	private static void setColumns(CommandLine cmd)
	 	{
	 		String checkColumns = cmd.getOptionValue('c').toString();
 			columns = checkColumns;
	 	}
	 	private static void setLimit(CommandLine cmd)
	 	{
	 		String checkLimit = cmd.getOptionValue('l').toString();
 			if(checkLimit.matches("[0-9]+")) 
 			{

 				limit = checkLimit;
 			}
 			else
 			{
 				System.out.println("Enter only positive integers for limit");
 				System.exit(5);
 			}
	 	}
	 	private static void setTime(CommandLine cmd, int decide)
	 	{
	 		String setTime;
	 		if(decide == 0)
	 		{
	 			 setTime = cmd.getOptionValue('m').toString();
	 		}
	 		else
	 		{
	 			 setTime = cmd.getOptionValue('M').toString();
	 		}
	 		
 			if(setTime.matches("-?[0-9]+")) 
 			{
 				if(decide == 0)
 				{
 					mintime = setTime;
 				}
 				else if(decide == 1)
 				{
 					maxTime = setTime;
 				}
 				
 			}
 			else
 			{
 				System.out.println("Enter only integers for Min/Max Time");
 				
 				System.exit(5);
 			}

	 	}
	 	private static void setEngine(CommandLine cmd)
	 	{
	 		String checkEngine = cmd.getOptionValue('e').toString();
 			if(checkEngine.equalsIgnoreCase("hive") || checkEngine.equalsIgnoreCase("presto"))
 			{
 				engine = checkEngine;
 			}
 			else
 			{
 				System.out.println("Please provide Hive or presto as an option for the engine");
 				System.exit(5);
 			}	
	 	}
	 	private static void verifyTime()
	 	{
	 		if(mintime != null && maxTime != null)
	 		{
	 			if(Long.parseLong(mintime)> Long.parseLong(maxTime))
	 			{
	 				System.out.println("Please ensure that the minimum time is lesser than the maximum time");
	 				System.exit(5);
	 			}
	 		}
	 	}
	 	
	 	private static String formatQuery()
		 {
			 String query = "select " + columns + " from " + table;
			 query =setTimeRange(query);
			 
			 if(limit != null)
			 {
				 query = query+" limit "+ limit +" "; 
			 }
			 return query;
		 }
		 private static String setTimeRange(String query)
		 {
			 if(mintime != null && maxTime == null)
			 {
				 query = query +" where TD_TIME_RANGE(time,"+mintime+", null)";
				 
			 }
			 else if (mintime == null && maxTime != null)
			 {
				 query = query +" where TD_TIME_RANGE(time,null, "+maxTime+")";
			 }
			 else if(mintime != null && maxTime != null)
			 {
				 query = query +" where TD_TIME_RANGE(time,"+ mintime +","+maxTime+")";
			 }
			 return query;
		 }
}

