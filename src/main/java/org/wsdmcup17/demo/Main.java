package org.wsdmcup17.demo;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

/**
 * A simple program demonstrating how to connect to the WSDM Cup data server,
 * and how to parse revisions and meta data.
 */
public class Main {
	
	private static final String
		WSDM_CUP_2017_CLIENT_DEMO = "WSDM Cup 2017 client demo:",
		OPT_SERVER = "s",
		OPT_SERVER_LONG = "server",
		OPT_SERVER_DESC = "Data server address",
		OPT_TOKEN = "t",
		OPT_TOKEN_LONG = "token",
		OPT_TOKEN_DESC = "Access token";

	private static final String
		LOG_PATTERN = "[%d{yyyy-MM-dd HH:mm:ss}] [%-5p] [%c{1}] %m%n",
		UTF_8 = "UTF-8";	
	
	public static void main(String[] args)
	throws UnknownHostException, IOException, URISyntaxException,
	InterruptedException {
		initLogger();
		
		CommandLine cmd = parseArgs(args);
		String serverAddress = cmd.getOptionValue(OPT_SERVER);
		String accessToken = cmd.getOptionValue(OPT_TOKEN);
		
		Client client = new Client(serverAddress, accessToken);
		client.start();
	}
	
	private static CommandLine parseArgs(String[] args){
		Options options = new Options();
	
		Option input = new Option(
				OPT_SERVER, OPT_SERVER_LONG, true, OPT_SERVER_DESC);
		input.setRequired(true);
		options.addOption(input);
		
		Option meta = new Option(
				OPT_TOKEN, OPT_TOKEN_LONG, true, OPT_TOKEN_DESC);
		meta.setRequired(true);
		options.addOption(meta);
	
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
	
		try {
			cmd = parser.parse(options, args);
		}
		catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp(WSDM_CUP_2017_CLIENT_DEMO, options);
			System.exit(1);
		}
		return cmd;	
	}
	
	private static void initLogger() {
		ConsoleAppender consoleAppender = new ConsoleAppender();
		consoleAppender.setEncoding(UTF_8);
		consoleAppender.setLayout(new PatternLayout(LOG_PATTERN));
		consoleAppender.setThreshold(Level.ALL);
		consoleAppender.activateOptions();
		org.apache.log4j.Logger.getRootLogger().addAppender(consoleAppender);
	}
}
