
package de.upb.wdqa.wsdmcup17.examples;

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
 * 
 * A Wikidata Toolkit {@link MwRevisionProcessor} that computes simple
 * statistics about the data set and scores all revisions 0.0, i.e.,
 * non-vandalism.
 *
 */
public class CorpusStatisticsProcessor implements MwRevisionProcessor {

	static final Logger logger = LoggerFactory.getLogger(CorpusStatisticsProcessor.class);

	CSVPrinter resultPrinter;

	BlockingQueue<CSVRecord> metaQueue;

	// for calculating the statistics
	int numberOfRevisions;
	int numberOfRegisteredRevisions;

	// for printing the progress every 10 seconds
	long lastTimeMillis;
	static final int PROGRESS_INTERVAL = 10000;

	public CorpusStatisticsProcessor(BlockingQueue<CSVRecord> metaQueue, CSVPrinter resultPrinter) {
		this.resultPrinter = resultPrinter;
		this.metaQueue = metaQueue;
	}

	public void startRevisionProcessing(String siteName, String baseUrl, Map<Integer, String> namespaces) {
		logger.info("Starting...");
		lastTimeMillis = System.currentTimeMillis();
	}

	public void processRevision(MwRevision mwRevision) {
		CSVRecord metaRecord = null;
		try {
			metaRecord = metaQueue.take();
			metaRecord.get("USER_COUNTRY_CODE");
		} catch (InterruptedException e) {
			logger.error("", e);
		}

		try {
			resultPrinter.print(mwRevision.getRevisionId());
			resultPrinter.print(0f);
			resultPrinter.println();
			resultPrinter.flush();
		} catch (IOException e) {
			logger.error("", e);
		}

		numberOfRevisions++;

		if (mwRevision.hasRegisteredContributor()) {
			numberOfRegisteredRevisions++;
		}

		long currentTimeMillis = System.currentTimeMillis();
		if (currentTimeMillis > lastTimeMillis + PROGRESS_INTERVAL) {
			logger.info("Current status:");
			printStatistics();
			lastTimeMillis = currentTimeMillis;
		}
	}

	public void finishRevisionProcessing() {
		try {
			resultPrinter.close();
		} catch (Throwable e) {
			logger.error("", e);
		}

		logger.info("Final result:");
		printStatistics();
	}

	private void printStatistics() {
		logger.info("   Number of revisions: " + numberOfRevisions);
		logger.info("   Number of registered revisions: " + numberOfRegisteredRevisions);
	}
}
