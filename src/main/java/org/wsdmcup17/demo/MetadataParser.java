package org.wsdmcup17.demo;

import java.io.IOException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetadataParser {
	
	private static final Logger
	LOG = LoggerFactory.getLogger(MetadataParser.class);

	private static final String
		UTF_8 = "UTF-8";
	
	public static final String
		REVISION_ID = "REVISION_ID",
		REVISION_SESSION_ID = "REVISION_SESSION_ID",
		USER_COUNTRY_CODE = "USER_COUNTRY_CODE",
		USER_CONTINENT_CODE = "USER_CONTINENT_CODE",
		USER_TIME_ZONE = "USER_TIME_ZONE",
		USER_REGION_CODE = "USER_REGION_CODE",
		USER_CITY_NAME = "USER_CITY_NAME",
		USER_COUNTY_NAME = "USER_COUNTY_NAME",
		REVISION_TAGS = "REVISION_TAGS";
	
	private static final String[]
		META_HEADER = {
			REVISION_ID,
			REVISION_SESSION_ID,
			USER_COUNTRY_CODE,
			USER_CONTINENT_CODE,
			USER_TIME_ZONE,
			USER_REGION_CODE,
			USER_CITY_NAME,
			USER_COUNTY_NAME,
			REVISION_TAGS
		};
	
	private static final CSVFormat
		CSV_FORMAT = CSVFormat.RFC4180.withHeader(META_HEADER);
	
	public static CSVRecord deserialize(byte[] bytes) throws IOException {
		String line = new String(bytes, UTF_8);
		return deserialize(line);
	}

	public static CSVRecord deserialize(String string) throws IOException {
		try{
			CSVParser parser = CSVParser.parse(string, CSV_FORMAT);
			return parser.getRecords().get(0);
		}
		catch (Throwable e){
			LOG.error("Unable to parse \"" + string + "\"", e);
			throw e;
		}
	}	
}
