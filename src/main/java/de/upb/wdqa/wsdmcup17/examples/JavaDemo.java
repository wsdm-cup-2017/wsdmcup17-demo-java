package de.upb.wdqa.wsdmcup17.examples;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Writer;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.wikidata.wdtk.dumpfiles.DumpContentType;
import org.wikidata.wdtk.dumpfiles.MwLocalDumpFile;
import org.wikidata.wdtk.dumpfiles.MwRevisionDumpFileProcessor;

/**
 * A simple program demonstrating how to connect to the WSDM Cup data server, and how to parse revisions and meta data.
 *
 */
public class JavaDemo 
{
	static final Logger logger = Logger.getLogger(JavaDemo.class);
	
	static final String LOGGER_PATTERN = "[%d{yyyy-MM-dd HH:mm:ss}] [%-5p] [%c{1}] %m%n";
	
	static final String[] RESULT_CSV_HEADER = {"REVISION_ID", "VANDALISM_SCORE"};

	private static final int REVISION_BUFFER_SIZE = 32 * 1024 * 1024;
	
	
    public static void main( String[] args ) throws UnknownHostException, IOException, URISyntaxException
    {
        CommandLine cmd = parseArgs(args);
        initLogger();
    	
        logger.info("Connecting to " + cmd.getOptionValue("d"));
        try(
        	Socket socket = getSocket(cmd.getOptionValue("d"));
        		
        	// output to data server	
        	OutputStream outputStream = socket.getOutputStream();
        	Writer outputWriter = new OutputStreamWriter(outputStream, "UTF-8");        		
        	 
        		
        	// internal communication between threads        		
        	PipedOutputStream revisionOutputStream= new PipedOutputStream();
        	PipedInputStream revisionInputStream = new PipedInputStream(revisionOutputStream, REVISION_BUFFER_SIZE);
        	
        	// input from data server
        	InputStream dataStreamInternal = socket.getInputStream();
        	DataInputStream dataStream = new DataInputStream(dataStreamInternal);
        	){
        	// authentication
        	outputWriter.write(cmd.getOptionValue("a") + "\r\n");
        	outputWriter.flush();
        	
        	try(CSVPrinter resultPrinter = new CSVPrinter(outputWriter, CSVFormat.RFC4180.withHeader(RESULT_CSV_HEADER))){
       	
        		computeStatistics(dataStream, revisionOutputStream, revisionInputStream, resultPrinter);
        	}
      	
        	}
    }
    
    private static void computeStatistics(DataInputStream dataStream, PipedOutputStream revisionOutputStream, PipedInputStream revisionInputStream, CSVPrinter resultPrinter){
		MwLocalDumpFile mwDumpFile = new MwLocalDumpFile("INPUT STREAM", DumpContentType.FULL, null, null);
		mwDumpFile.prepareDumpFile();
		
		BlockingQueue<CSVRecord> queue = new ArrayBlockingQueue<>(128);
		Thread demultiplexerThread = new Thread(new Demultiplexer(dataStream, queue, revisionOutputStream), "Demultiplexer");
		demultiplexerThread.start();

		MwRevisionDumpFileProcessor dumpFileProcessor = new MwRevisionDumpFileProcessor(
				new CorpusStatisticsProcessor(queue, resultPrinter));
		
		// Remark: The dumpFileProcessor closes the inputStream and thus the socket.
		dumpFileProcessor.processDumpFileContents(revisionInputStream, mwDumpFile); 
		
		try {
			demultiplexerThread.join();
		} catch (InterruptedException e) {
			logger.error("", e);
		}	
    }
    
    static Socket getSocket(String address) throws URISyntaxException, UnknownHostException, IOException{
  	  URI uri = new URI("my://" + address);
  	  String host = uri.getHost();
  	  int port = uri.getPort();
  	  
   	  return new Socket(host, port);
    }
    
    static CommandLine parseArgs(String[] args){
	    Options options = new Options();
	
	    Option input = new Option("d", "input", true, "data server address");
	    input.setRequired(true);
	    options.addOption(input);
	    
	    Option meta = new Option("a", "auth", true, "authentication token");
	    meta.setRequired(true);
	    options.addOption(meta);
	
	    CommandLineParser parser = new DefaultParser();
	    HelpFormatter formatter = new HelpFormatter();
	    CommandLine cmd = null;
	
	    try {
	        cmd = parser.parse(options, args);
	    } catch (ParseException e) {
	        System.out.println(e.getMessage());
	        formatter.printHelp("java-demo", options);
	
	        System.exit(1);
	    }
		return cmd;    
    }
    
	/**
	 * Initializes the logger that is used during the processing of the corpus.
	 */
	private static void initLogger() {
		ConsoleAppender consoleAppender = new ConsoleAppender();
		consoleAppender.setEncoding("UTF-8");
		consoleAppender.setLayout(new PatternLayout(LOGGER_PATTERN));
		consoleAppender.setThreshold(Level.ALL);
		consoleAppender.activateOptions();
		org.apache.log4j.Logger.getRootLogger().addAppender(consoleAppender);
	}
    
}
