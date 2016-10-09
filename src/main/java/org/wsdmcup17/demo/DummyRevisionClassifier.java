
package org.wsdmcup17.demo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.wdtk.dumpfiles.MwRevision;
import org.wikidata.wdtk.dumpfiles.MwRevisionProcessor;

/**
 * A Wikidata Toolkit-based revision processor that classifies all revisions
 * as non-vandalism and hence sends a score 0f 0.0 to the server for each
 * revision.
 */
public class DummyRevisionClassifier implements MwRevisionProcessor {

	private static final Logger
		LOG = LoggerFactory.getLogger(DummyRevisionClassifier.class);
	
	private static final String
		LOG_MSG_STARTING = "Starting...",
		LOG_MSG_CURRENT_STATUS = "Current status:",
		LOG_MSG_FINAL_RESULT = "Final result:",
		LOG_MSG_NUM_REVISIONS = "   Number of revisions: %s",
		LOG_MSG_NUM_REVISIONS_REGISTERED = 
			"   Number of registered revisions: %s";

	private static final int
		LOG_STATISTICS_INTERVAL = 10000;
	
	private CSVPrinter resultPrinter;
	private BlockingQueue<CSVRecord> metadataQueue;
	private long lastLogTime;
	private int numRevisions;
	private int numRevisionsFromRegisteredUsers;

	public DummyRevisionClassifier(
		BlockingQueue<CSVRecord> metaQueue, CSVPrinter resultPrinter
	) {
		this.resultPrinter = resultPrinter;
		this.metadataQueue = metaQueue;
	}

	@Override
	public void startRevisionProcessing(
		String siteName, String baseUrl, Map<Integer, String> namespaces
	) {
		LOG.info(LOG_MSG_STARTING);
		lastLogTime = System.currentTimeMillis();
	}

	@Override
	public void processRevision(MwRevision revision) {
		// Retrieve corresponding metadata from metadata queue.
		CSVRecord metadata = getMetadata();

		// Classify revision
		float classificationScore = classifyRevision(revision, metadata);
		
		// Send result so server.
		sendClassificationResult(revision.getRevisionId(), classificationScore);
		
		// Update statistics for logging.
		updateStatistics(revision, metadata);
	}
	
	private float classifyRevision(MwRevision revision, CSVRecord metadata) {
		// This is where an actual classification based on  the revision and
		// its associated metadata should happen. Instead, we just assign a
		// score of 0.0, effectively classifying the revision as non-vandalism.
		return 0.0f;
	}

	private void sendClassificationResult(
		long revisionId, float classificationScore
	) {
		try {
			resultPrinter.print(revisionId);
			resultPrinter.print(classificationScore);
			resultPrinter.println();
			resultPrinter.flush();
		}
		catch (IOException e) {
			LOG.error("", e);
		}
	}
	
	private CSVRecord getMetadata() {
		try {
			return metadataQueue.take();
		}
		catch (InterruptedException e) {
			LOG.error("", e);
			return null;
		}
	}

	@Override
	public void finishRevisionProcessing() {
		try {
			resultPrinter.close();
		}
		catch (IOException e) {
			LOG.error("", e);
		}
		LOG.info(LOG_MSG_FINAL_RESULT);
		printStatistics();
	}

	private void updateStatistics(MwRevision mwRevision, CSVRecord metadata) {
		numRevisions++;
		if (mwRevision.hasRegisteredContributor()) {
			numRevisionsFromRegisteredUsers++;
		}
		long currentTime = System.currentTimeMillis();
		if (currentTime > lastLogTime + LOG_STATISTICS_INTERVAL) {
			LOG.info(LOG_MSG_CURRENT_STATUS);
			printStatistics();
			lastLogTime = currentTime;
		}
	}
	
	private void printStatistics() {
		LOG.info(String.format(LOG_MSG_NUM_REVISIONS, numRevisions));
		LOG.info(String.format(LOG_MSG_NUM_REVISIONS_REGISTERED,
				numRevisionsFromRegisteredUsers));
	}
}
