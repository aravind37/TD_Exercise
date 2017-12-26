import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalTime;
import java.util.HashSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Function;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpUnauthorizedException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJob.Status;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;

public class QueryTable {
	
	//Declaring constants to be used
	
	private static final String DEFAULT_OUTPUT_FORMAT="tabular";
	private static final String DEFAULT_ENGINE_TYPE="presto";
	private static final String DEFAULT_COLUMNS = "*";
	private static final String ERR_MSG_AUTHORIZATION_EXCEPTION="We are unable to authenticate. Please ensure that the td.conf file has been set correctly";
	private static final String ERR_MSG_CONN_EXCEPTION="There is an issue with establishing connection with Treasure Data, please verify if you have configuration right";
	private static final String ERR_MSG_QUERY_ENGINE = "Sorry the query engine you are trying to use is not enabled for this account";
	private static final String MSG_SUCCESS = "Your request has been completed successfully";
	private static final String ERR_MSG_WRONG_COLUMNS="Sorry the columns that you have entered is not present in the table";
	private static final String ERR_MSG_INVALID_DB= "Sorry the DB that your provided does not exist";
	private static final String ERR_MSG_INVALID_TABLE ="Sorry the table the you have mentioned does not exist in the database";
	private static final String ERR_MSG_INVALID_OPTIONS="Please provide two required arguments, the DB name and the Table name";
	private static final String ERR_MSG_INVALID_FORMAT="Please provide Tabular or CSV as an option for Format";
	private static final String ERR_MSG_INVALID_LIMIT="Enter only positive integers for limit";
	private static final String ERR_MSG_INVALID_TIME="Enter only integers for Min/MAX Time";
	private static final String ERR_MSG_INVLAID_ENGINE="Please provide Hive or presto as an option for the engine";
	private static final String ERR_MSG_MIN_MAX_COMPARE = "Please ensure that Min is lesser than MAX";
	
	private static final int ERR_CODE_TD = -1;
	private static final int ERR_CODE_USER = 1;
	private static final int CODE_SUCCESS = 0;
	
	static String format = DEFAULT_OUTPUT_FORMAT;
	static String columns = DEFAULT_COLUMNS;
	static String limit = null;
	static String minTime = null;
	static String maxTime = null;
	static String engine = DEFAULT_ENGINE_TYPE;
	static String database = null;
	static String table = null;
	static TDClient client=null;
	static String query = null;
	


	public static void main(String[] args) throws InterruptedException {
		Options commandLineOptions = buildOptions();

		// validate the options and assign to the respective variables
		setOptions(commandLineOptions, args);

		// establish a new TDclient connection. Print a message if the
		// configuration is invalid
		try {
			client = TDClient.newClient();
			// validate if the the DB name and table name are valid
			validateDatabase(client);
			validateTable(client);
			validateColumns(client);
		} 
		catch(TDClientHttpUnauthorizedException TDex){
			closeClient(client,ERR_MSG_AUTHORIZATION_EXCEPTION,ERR_CODE_TD );
		}
		catch (Exception ex) {
			closeClient(client, ERR_MSG_CONN_EXCEPTION, ERR_CODE_TD);
		}
				
		// format the query that we will run based on the parameters provided in
		// the command line
		 query = formatQuery();
		System.out.println("Going to run the query " + query);
		//Decide if the query has to be run on Hive or presto engine
		TDJobRequest dbType = setEngineType();
		String jobId = null;
		try {
			jobId = client.submit(dbType);
		} catch (TDClientException ex) {
			// Presto queries are not enabled for trial account
			closeClient(client, ERR_MSG_QUERY_ENGINE, ERR_CODE_TD);
		}

		// Decide the format of report based on users selection criteria
		TDResultFormat reportFormat=setReportType();
		TDJobSummary job = client.jobStatus(jobId);
		// wait until the query is executed
		ExponentialBackOff backOff = new ExponentialBackOff();
		while (!job.getStatus().isFinished()) {
			System.out.println("Query is executing");
			Thread.sleep(backOff.nextWaitTimeMillis());
			job = client.jobStatus(jobId);
		}
		// Read the detailed job information
		TDJob jobInfo = client.jobInfo(jobId);
		// get the jobStatus
		Status status = job.getStatus();
		// set the output format
		if (status == job.getStatus().SUCCESS) {
			if (format.equalsIgnoreCase("tabular")) {
				printTabularOutput(jobId, reportFormat);
			} else {
				saveToCSV(jobId, reportFormat);
			}
		} else {
			closeClient(client, jobInfo.getCmdOut(), ERR_CODE_TD);
		}

	}
	/**
	 * Method to set the query engine type
	 * @return - returns EngineType
	 */
	private static TDJobRequest setEngineType()
	{
		TDJobRequest dbType;
		if (engine.equalsIgnoreCase("Hive")) {
			dbType = TDJobRequest.newHiveQuery(database, query);
		} else {
			dbType = TDJobRequest.newPrestoQuery(database, query);
		}
		return dbType;
	}

	/**
	 * Method to set the report type, options can be CSV or tabular
	 * @return - Returns TDResultFormat object with the appropriate return type
	 */
	private static TDResultFormat setReportType()
	{
		TDResultFormat reportFormat;
		if (format.equalsIgnoreCase("csv")) {
			reportFormat = TDResultFormat.CSV;
		} else {
			reportFormat = TDResultFormat.TSV;
		}
		return reportFormat;
	}
	
	/**
	 * Function to print the query in tabluar format
	 * @param jobId - The Job Id of the query that is being executed
	 * @param resultFormat - The output format of the result that user has specificed
	 */
	private static void printTabularOutput(String jobId, TDResultFormat resultFormat) {
		client.jobResult(jobId, resultFormat, new Function<InputStream, Boolean>() {

			public Boolean apply(InputStream input) {
				try {
					String resultString = new String(ByteStreams.toByteArray(input));

					// verify if the query has returned any output
					if (resultString.length() > 0) {
						System.out.println(resultString);
						// System.out.println("Exiting....");

					} else {
						System.out.println("Sorry, the query did not return any result");

					}
					closeClient(client, MSG_SUCCESS, CODE_SUCCESS);
					
				} catch (Exception e) {
					closeClient(client, e.toString(), ERR_CODE_TD);
				}

				return true;

			}
		});
	}
	/**
	 * Method to save the output to a csv file
	 * @param jobId - The Job Id of the query that is being executed
	 * @param resultFormat - Output format specified by the user
	 */
	private static void saveToCSV(String jobId, TDResultFormat resultFormat) {
		client.jobResult(jobId, resultFormat, new Function<InputStream, Boolean>() {
			@Override
			public Boolean apply(InputStream input) {
				try {
					byte[] buffer = ByteStreams.toByteArray(input);
					if (buffer.length > 0) {
						// Create a new file in the directory where the function
						// is being executed
						File targetFile = new File("Query_Result_"+LocalTime.now()+".csv");

						// Print the location of the file where the CSV will be
						// saved
						System.out.println("The CSV file the name Query_Result.csv is being saved at the location:"
								+ targetFile.getCanonicalPath());
						Files.write(buffer, targetFile);

					} else {
						System.out.println("Sorry the query did not return any results");

					}
					// close the connection and exit
					closeClient(client, MSG_SUCCESS, CODE_SUCCESS);

				} catch (IOException e) {
					// Catch and print exception related to files
					closeClient(client, e.toString(), ERR_CODE_TD);
				}

				return true;

			}
		});

	}
/**
 * Method to validate if the columns provided are valid
 * @param client - Takes TDClient as input
 */
	private static void validateColumns(TDClient client) {
		if (columns != "*") {
			Object[] columns_table = client.showTable("test_db", "demo").getColumns().toArray();

			String[] columns_name = new String[columns_table.length];
			for (int i = 0; i < columns_table.length; i++) {
				String[] parse_name = columns_table[i].toString().split(":");
				columns_name[i] = parse_name[0];
			}
			String[] columns_input = columns.split(",");

			// Create a hashlist

			HashSet<String> hash_Columns = new HashSet<>();

			for (int i = 0; i < columns_name.length; i++) {
				if (!hash_Columns.contains(columns_name[i]))
					hash_Columns.add(columns_name[i]);
			}
			for (int i = 0; i < columns_input.length; i++) {
				if (!hash_Columns.contains(columns_input[i]) && columns_input.length <= columns_name.length)
					 {
					closeClient(client, ERR_MSG_WRONG_COLUMNS, ERR_CODE_USER);
				}
			}
		}

	}

	
	/**
	 * Method to validate if the Database provided is valid
	 * @param client - Takes TDClient as input
	 */
	private static void validateDatabase(TDClient client) {
		if (!client.existsDatabase(database)) {
			closeClient(client, ERR_MSG_INVALID_DB, ERR_CODE_TD);
		}
	}

	/**
	 * Method to validate if the table provided is valid
	 * @param client - Takes TDClient as input
	 */
	private static void validateTable(TDClient client) {
		if (!client.existsTable(database, table)) {
			closeClient(client, ERR_MSG_INVALID_TABLE, ERR_CODE_TD);
		}
	}

	/**
	 * Method to build the command line options
	 * @return
	 */
	private static Options buildOptions() {
		Options option = new Options();
		option.addOption("f", "format", true,
				"Format of the output : enter either CSV or Tabular, ex: -f/--format csv");
		option.addOption("c", "columns", true,
				"Provide the list of the columns that you want to query, ex: -c/--columns 'age' ");
		option.addOption("l", "limit", true, "Limit to output to few rows, ex -l/--limt 100");
		option.addOption("m", "min", true, "Enter the minimum time stamp, ex: -m/--min <unixTimeStamp>");
		option.addOption("M", "MAX", true, "Enter the maximum time stap,ex: -M/--MAX <unixTimeStamp>");
		option.addOption("e", "engine", true,
				"Provide the engine type, you can choose between hive or presto,ex: -e/--engine hive ");

		return option;

	}
	/**
	 * Method to set program options
	 * @param options - This is the command line parameters allowed
	 * @param args - The command line arguments provided
	 */

	static void setOptions(Options options, String args[]) {
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		HelpFormatter formatter = new HelpFormatter();

		try {
			cmd = parser.parse(options, args);
			//
			// System.out.println("The output value is " +checkFormat);
			if (cmd.getOptionValue('f') != null) {
				setFormatType(cmd);

			}

			if (cmd.getOptionValue('c') != null) {
				setColumns(cmd);
			}

			if (cmd.getOptionValue('l') != null) {
				setLimit(cmd);
			}

			if (cmd.getOptionValue('m') != null) {
				setTime(cmd, 0);

			}

			if (cmd.getOptionValue('M') != null) {
				setTime(cmd, 1);
			}
			verifyTime();

			if (cmd.getOptionValue('e') != null) {
				setEngine(cmd);
			}
			Object[] getDBTable = cmd.getArgList().toArray();
			if (getDBTable.length < 2 || getDBTable.length > 2) {
				closeClient(null, ERR_MSG_INVALID_OPTIONS, ERR_CODE_USER);
			}

			// get the DB and Table names from the string
			database = getDBTable[0].toString();
			table = getDBTable[1].toString();

		} catch (ParseException e) {

			// If the arguments are wrong or invalid, print a help message
			// providing actual syntax
			formatter.printHelp(
					"query_Table.bat/query_Table.sh <yourDB> <yourTable> (optional argumets with flags as listed below)",
					options);

			System.exit(1);
		}

	}

	/**
	 * Set the output format
	 * @param cmd - Get the command line argument as input
	 */
			
	private static void setFormatType(CommandLine cmd) {
		String checkFormat = cmd.getOptionValue('f').toString();
		if (checkFormat.equalsIgnoreCase("tabular") || checkFormat.equalsIgnoreCase("CSV")) {
			format = checkFormat;
		} else {
			closeClient(null,ERR_MSG_INVALID_FORMAT,ERR_CODE_TD);
		}
	}

	/**
	 * Set the columns that the query should execute
	 * @param cmd - Get the command line argument as input
	 */
	private static void setColumns(CommandLine cmd) {
		String checkColumns = cmd.getOptionValue('c').toString();
		columns = checkColumns;
	}
	/**
	 * Limit the number of rows
	 * @param cmd - Get the command line argument as input
	 */

	private static void setLimit(CommandLine cmd) {
		String checkLimit = cmd.getOptionValue('l').toString();
		if (checkLimit.matches("[0-9]+")) {

			limit = checkLimit;
		} else {
			closeClient(null, ERR_MSG_INVALID_LIMIT, ERR_CODE_USER);
		}
	}

	/**
	 * Set the Min and MAX value for the time
	 * @param cmd - Provide the values entered
	 * @param decide - Set Min or Max based on the decide value
	 */
	private static void setTime(CommandLine cmd, int decide) {
		String setTime;
		if (decide == 0) {
			setTime = cmd.getOptionValue('m').toString();
		} else {
			setTime = cmd.getOptionValue('M').toString();
		}

		if (setTime.matches("-?[0-9]+")) {
			if (decide == 0) {
				minTime = setTime;
			} else if (decide == 1) {
				maxTime = setTime;
			}

		} else {
				closeClient(null,ERR_MSG_INVALID_TIME, ERR_CODE_USER);
		}

	}

	/**
	 * Set the query engine type
	 * @param cmd - Takes command line as the argument
	 */
	private static void setEngine(CommandLine cmd) {
		String checkEngine = cmd.getOptionValue('e').toString();
		if (checkEngine.equalsIgnoreCase("hive") || checkEngine.equalsIgnoreCase("presto")) {
			engine = checkEngine;
		} else {
				closeClient(null, ERR_MSG_INVLAID_ENGINE, ERR_CODE_USER);
		}
	}

	/**
	 * Verify if Min is less than Max 
	 */
	private static void verifyTime() {
		if (minTime != null && maxTime != null) {
			if (Long.parseLong(minTime) > Long.parseLong(maxTime)) {
				closeClient(null, ERR_MSG_MIN_MAX_COMPARE, ERR_CODE_USER);
			}
		}
	}
/**
 * Format the query based on Limit
 * @return
 */
	private static String formatQuery() {
		String query = "select " + columns + " from " + table;
		query = setTimeRange(query);

		if (limit != null) {
			query = query + " limit " + limit + " ";
		}
		return query;
	}

	/**
	 * Update the query if Min and/or Max are provided
	 * @param query - the query string
	 * @return
	 */
	private static String setTimeRange(String query) {
		if (minTime != null && maxTime == null) {
			query = query + " where TD_TIME_RANGE(time," + minTime + ", null)";

		} else if (minTime == null && maxTime != null) {
			query = query + " where TD_TIME_RANGE(time,null, " + maxTime + ")";
		} else if (minTime != null && maxTime != null) {
			query = query + " where TD_TIME_RANGE(time," + minTime + "," + maxTime + ")";
		}
		return query;
	}
	
	/**
	 * Method to handle the closing of connection to TD client and exiting with proper error code
	 * @param client - This is the TD Client
	 * @param message -Message to be displayed
	 * @param exit_code - The code with which the program has to exit. Exit code is 1 for user errors, -1 when the issue on TD Client side and 0 when successfull
	 */
	private static void closeClient(TDClient client, String message, int exit_code)
	{
		if(client != null)
		{
			client.close();
		}
		System.out.println(message);
		System.exit(exit_code);
	}
}
