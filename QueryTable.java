import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJob.Status;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;

public class QueryTable {

	static String format = "TABULAR";
	static String columns = "*";
	static String limit = null;
	static String minTime = null;
	static String maxTime = null;
	static String engine = "presto";
	static String database = null;
	static String table = null;
	static TDClient client;

	public static void main(String[] args) throws InterruptedException {
		Options commandLineOptions = buildOptions();

		// validate the options and assign to the respective variables
		setOptions(commandLineOptions, args);

		// establish a new TDclient connection. Print a message if the
		// configuration is invalid
		try {
			client = TDClient.newClient();
		} catch (Exception ex) {
			System.out.println(
					"There is an issue with establishing connection with Treasure Data, please verify if you have configuration right");
		}

		// validate if the the DB name and table name are valid
		validateDatabase(client);
		validateTable(client);
		validateColumns(client);

		// format the query that we will run based on the parameters provided in
		// the command line
		String query = formatQuery();

		System.out.println("Going to run the query " + query);
		TDJobRequest dbType = null;

		// Decide to run Hive or Presto Query
		if (engine.equalsIgnoreCase("Hive")) {
			dbType = TDJobRequest.newHiveQuery(database, query);
		} else {
			dbType = TDJobRequest.newPrestoQuery(database, query);
		}

		String jobId = null;
		try {
			jobId = client.submit(dbType);
		} catch (Exception ex) {
			// Presto queries are not enabled for trial account
			System.out.println("Sorry Presto Queries are not enabled for this account");
			System.exit(-1);
		}

		// Decide the format of report based on users selection criteria
		TDResultFormat reportFormat;

		if (format.equalsIgnoreCase("csv")) {
			reportFormat = TDResultFormat.CSV;
		} else {
			reportFormat = TDResultFormat.TSV;
		}

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
			System.out.println(jobInfo.getCmdOut());
			// close the client connection after the query fails
			client.close();
			System.exit(-1);
		}

	}

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
					client.close();
					System.exit(0);

				} catch (Exception e) {
					System.out.println(e.getStackTrace());
					client.close();
					System.exit(-1);
				}

				return true;

			}
		});
	}

	private static void saveToCSV(String jobId, TDResultFormat resultFormat) {
		client.jobResult(jobId, resultFormat, new Function<InputStream, Boolean>() {
			@Override
			public Boolean apply(InputStream input) {
				try {
					byte[] buffer = ByteStreams.toByteArray(input);
					if (buffer.length > 0) {
						// Create a new file in the directory where the function
						// is being executed
						File targetFile = new File("Query_Result.csv");

						// Print the location of the file where the CSV will be
						// saved
						System.out.println("The CSV file the name Query_Result.csv is being saved at the location:"
								+ targetFile.getCanonicalPath());
						Files.write(buffer, targetFile);

					} else {
						System.out.println("Sorry the query did not return any results");

					}
					// close the connection and exit
					client.close();
					System.exit(0);

				} catch (IOException e) {
					// Catch and print exception related to files
					e.printStackTrace();
					// close the connection after throwing the error message
					client.close();
					System.exit(-1);
				}

				return true;

			}
		});

	}

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
				if (hash_Columns.contains(columns_input[i]) && columns_input.length <= columns_name.length)
					;
				else {
					System.out.println("Sorry the columns that you have entered is not present in the table");
					System.out.print("The value(s) for the column(s) can contain the following value(s):");
					for (String column : columns_name) {
						System.out.print(column + ",");
					}
					client.close();
					System.exit(1);
					;
				}
			}
		}

	}

	// verify if the DB provided in the argument is valid
	private static void validateDatabase(TDClient client) {
		// System.out.println(client.showTable("test_db",
		// "demo").getColumns().toArray().toString());

		if (client.existsDatabase(database)) {
			;
		} else {
			client.close();
			System.out
					.println("Please provide a valid Database name. The Database base " + database + " does not exist");
			System.exit(-1);
		}
	}

	// validate if table provided in argument is valid
	private static void validateTable(TDClient client) {
		if (client.existsTable(database, table)) {
			;
		} else {
			client.close();
			System.out.println("Please provide a valid table name. The table  " + table
					+ " does not exist in the Database " + database);
			System.exit(-1);
		}
	}

	// Define command line options
	private static Options buildOptions() {
		Options option = new Options();
		option.addOption("f", "format", true,
				"Format of the output : enter either CSV or Tabular, ex: -f/--format csvr");
		option.addOption("c", "columns", true,
				"Provide the list of the columns that you want to query, ex: -c/--columns 'age' ");
		option.addOption("l", "limit", true, "Limit to output to few rows, ex -l/--limt 100");
		option.addOption("m", "min", true, "Enter the minimum time stamp, ex: -m/--min <unixTimeStamp>");
		option.addOption("M", "MAX", true, "Enter the maximum time stap,ex: -M/--MAX <unixTimeStamp>");
		option.addOption("e", "engine", true,
				"Provide the engine type, you can choose between hive or presto,ex: -e/--engine hive ");

		return option;

	}

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
				System.out.println("Please provide two required arguments, the DB name and the Table name");
				System.exit(1);
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

	private static void setFormatType(CommandLine cmd) {
		String checkFormat = cmd.getOptionValue('f').toString();
		if (checkFormat.equalsIgnoreCase("tabular") || checkFormat.equalsIgnoreCase("CSV")) {
			format = checkFormat;
		} else {
			System.out.println("Please provide Tabular or CSV as an option for Format");
			System.exit(1);
		}
	}

	private static void setColumns(CommandLine cmd) {
		String checkColumns = cmd.getOptionValue('c').toString();
		columns = checkColumns;
	}

	private static void setLimit(CommandLine cmd) {
		String checkLimit = cmd.getOptionValue('l').toString();
		if (checkLimit.matches("[0-9]+")) {

			limit = checkLimit;
		} else {
			System.out.println("Enter only positive integers for limit");
			System.exit(1);
		}
	}

	// Set min or MAX value
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
			System.out.println("Enter only integers for Min/MAX Time");

			System.exit(1);
		}

	}

	private static void setEngine(CommandLine cmd) {
		String checkEngine = cmd.getOptionValue('e').toString();
		if (checkEngine.equalsIgnoreCase("hive") || checkEngine.equalsIgnoreCase("presto")) {
			engine = checkEngine;
		} else {
			System.out.println("Please provide Hive or presto as an option for the engine");
			System.exit(1);
		}
	}

	private static void verifyTime() {
		if (minTime != null && maxTime != null) {
			if (Long.parseLong(minTime) > Long.parseLong(maxTime)) {
				System.out.println("Please ensure that the minimum time is lesser than the maximum time");
				System.exit(1);
			}
		}
	}

	private static String formatQuery() {
		String query = "select " + columns + " from " + table;
		query = setTimeRange(query);

		if (limit != null) {
			query = query + " limit " + limit + " ";
		}
		return query;
	}

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
}
