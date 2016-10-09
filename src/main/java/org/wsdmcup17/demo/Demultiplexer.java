package org.wsdmcup17.demo;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

/**
 * Thread to demultiplex revisions and meta data. The resulting revisions are
 * provided as an {@link OutputStream} that can, for example, be processed with
 * Wikidata Toolkit. The metadata is parsed and put in a queue for further
 * processing.
 */
public class Demultiplexer implements Runnable {
	
	private static final Logger
		LOG = Logger.getLogger(Demultiplexer.class);
	
	private static final String
		LOG_MSG_END_OF_STREAM = "End of stream.",
		LOG_MSG_METADATA_WITHOUT_REVISION = "Metadata without revision for %s";
	
	private BlockingQueue<CSVRecord> metadataQueue;
	private DataInputStream dataStream;
	private PipedOutputStream revisionOutputStream;
	
	public Demultiplexer(
		DataInputStream inputStream, BlockingQueue<CSVRecord> metaQueue,
		PipedOutputStream revisionOutputStream
	) {
		this.dataStream = inputStream;
		this.metadataQueue = metaQueue;
		this.revisionOutputStream = revisionOutputStream;		
	}
	
	@Override
	public void run() {
		try {
			demultiplexStream();
		}
		catch (IOException | InterruptedException e) {
			LOG.error("", e);
		}
	}

	private void demultiplexStream() throws IOException, InterruptedException {
		try {
			while (true) {
				// Read metadata from stream and queue it.
				byte[] bytes = readNextItem(dataStream);
				if (bytes == null) {
					break;
				}
				CSVRecord metadata = MetadataParser.deserialize(bytes);	
				metadataQueue.put(metadata);
				
				// Read corresponding revision from stream and forward it.
				bytes = readNextItem(dataStream);
				if (bytes == null) {
					logStreamInconsistencyError(metadata);
					break;
				}
				else {
					revisionOutputStream.write(bytes);
					revisionOutputStream.flush();
				}
			}
		}
		finally {
			this.revisionOutputStream.close();
		}
	}
	
	private static byte[] readNextItem(DataInputStream dataStream)
	throws IOException {
		try {
			int length = dataStream.readInt();
			byte[] bytes = new byte[length];
			dataStream.readFully(bytes);
			return bytes;
		}
		catch (EOFException e) {
			LOG.info(LOG_MSG_END_OF_STREAM);
			return null;
		}
	}
	
	private void logStreamInconsistencyError(CSVRecord metadata) {
		String revisionId = metadata.get(MetadataParser.REVISION_ID);
		LOG.error(String.format(LOG_MSG_METADATA_WITHOUT_REVISION, revisionId));
	}
}
