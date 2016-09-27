package de.upb.wdqa.wsdmcup17.examples;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

/**
 * Thread demultiplexing revisions and meta data.
 * The resulting revisions are provided as an {@link OutputStream} that can possibly be processed with Wikidata Toolkit.
 * The meta data is parsed and put in a queue for furhter processing. 
 *
 */
public class Demultiplexer implements Runnable {
	
	static Logger logger = Logger.getLogger(Demultiplexer.class);
	
	static final String[] META_HEADER = {
			"REVISION_ID",
			"REVISION_SESSION_ID",
			"USER_COUNTRY_CODE",
			"USER_CONTINENT_CODE",
			"USER_TIME_ZONE",
			"USER_REGION_CODE",
			"USER_CITY_NAME",
			"USER_COUNTY_NAME",
			"REVISION_TAGS" };
	
	BlockingQueue<CSVRecord> metaQueue;
	DataInputStream inputStream;
	PipedOutputStream revisionOutputStream;
	
	public Demultiplexer(DataInputStream inputStream, BlockingQueue<CSVRecord> metaQueue, PipedOutputStream revisionOutputStream ){
		this.inputStream = inputStream;
		this.metaQueue = metaQueue;
		this.revisionOutputStream = revisionOutputStream;		
	}
	
	
	@Override
	public void run() {
		try {
		
			byte[] bytes;
			do{
				bytes = readBinaryItem(inputStream);
				if (bytes != null){	
					processMetaData(bytes);	
				}
				bytes = readBinaryItem(inputStream);
				if (bytes != null){
					processRevision(bytes);
				}
			}while(bytes!= null);	
		
		} catch (IOException | InterruptedException e) {
			logger.error("", e);
		}
		
		try {
			this.revisionOutputStream.close();
		} catch (IOException e) {
			logger.error("", e);
		}

		
	}
	
	private static byte[] readBinaryItem(DataInputStream inputStream){
		try{
			int length = inputStream.readInt();
			byte[] bytes = new byte[length];
			inputStream.readFully(bytes);
			return bytes;
		}
		catch (EOFException e){
			return null;
		}
		catch(IOException e){
			logger.error("", e);
			return null;
		}
	}
	
	private void processRevision(byte[] bytes) throws IOException{
		revisionOutputStream.write(bytes);
		revisionOutputStream.flush();
	}
	
	private void processMetaData(byte[] bytes) throws IOException, InterruptedException{
		String line = new String(bytes, "UTF-8"); 
		CSVRecord record = parseLine(line);
		metaQueue.put(record);
	}
	
	private CSVRecord parseLine(String line) throws IOException{
		CSVParser parser = CSVParser.parse(line, CSVFormat.RFC4180.withHeader(META_HEADER));
		return parser.getRecords().get(0);
	}
}
