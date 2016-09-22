package com.vinhuang.sample.tasklets;

// Import appropriate packages

public class ExecuteSQLByXmlTasklet implements Tasklet, StepExecutionListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteSQLByXmlTasklet.class);
	private String xmlLocation;
	private DataSource dataSource;
	private NamedParameterJdbcTemplate jdbcTemplate;
	private Map<String, Integer> outcome = new TreeMap<String, Integer>(new KeyComparison());

	/**
	 * Before step will run prior to processing. Logging can be added here.
	 *
	 * @param StepExecution stepExecution
	 * @return
	*/
	@Override
	@BeforeStep
	public void beforeStep(StepExecution stepExecution) {
		super.beforeStep(stepExecution);
	}

	/**
	 * After step will run after to processing. Results/summary can be added here.
	 *
	 * @param StepExecution stepExecution
	 * @return ExitStatus
	*/
	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		for(Map.Entry<String, Integer> record : this.outcome.entrySet()) {
			LOGGER.info("Number of record(s) affected for SQL key {} is {}", record.getKey(), record.getValue());
		}
		return null;
	}

	/**
	 * Begin processing. Summary processing:
	 * -- Generate SQL queries from the XML input file
	 * -- Move the input file to archive
	 * -- Run each queries against SQL database
	 * -- Retrieve results
	 * Note: this method works best for insert/update/delete queries. Not so well on Select queries
	 * @param StepContribution contribution
	 * @param ChunkContext chunkContext
	 * @return ExitStatus
	*/
	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
		try {
			Map<String, String> sqlQueries = this.generateQueries();
			this.moveToArchive();
			for(Map.Entry<String, String> sql : sqlQueries.entrySet()) {
				long start = System.currentTimeMillis();
				int updateCount = this.runQuery(sql.getKey(), sql.getValue());
				long end = System.currentTimeMillis();
				long delta = end - start;
				LOGGER.info("Run time for key {}: {}", sql.getKey(), this.convertToString(delta));
				this.outcome.put(sql.getKey(), updateCount);
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	/**
	 * Convert run time tracking to String for logging purposes
	 * @param long delta
	 * @return String
	*/
	public String convertToString(long delta) {
		long minute = delta / 60000;
		double second = Double.valueOf(delta) / 1000;
		StringBuilder sb = new StringBuilder();
		sb.append(delta);
		sb.append("ms (");
		sb.append(minute);
		sb.append("m ");
		sb.append(second);
		sb.append("s)");
		return sb.toString();
	}

	/**
	 * Generate queries from the XML input file by mapping the key
	 * and the queries to a Map.
	 * Note: this method throws generic Exception for simplicity.
	 * Exception can be modified accordingly to follow best practice
	 * @return Map<String, String>
	*/
	public Map<String, String> generateQueries() throws Exception {
		Properties queries = new Properties();
		Map<String, String> results = new TreeMap <String, String>(new KeyComparison());
		InputStream inStream = null;
		try {
			inStream = new FileInputStream(this.xmlLocation);
			queries.loadFromXML(inStream);
			for(Object key : queries.keySet()) {
				results.put(String.valueOf(key), queries.getProperty(String.valueOf(key)));
			}
		} catch(FileNotFoundException e) {
			String errorMessage = "Input file doesn't exist";
			this.logError(LOGGER, errorMessage);
			throw new RuntimeException(e);
		} catch(IOException e) {
			String errorMessage = "Input file cannot be read";
			this.logError(LOGGER, errorMessage);
			throw new RuntimeException(e);
		} catch(ClassCastException e) {
			this.logError(LOGGER, errorMessage);
			throw new RuntimeException(e);
		} catch(UnsupportedOperationException e) {
			this.logError(LOGGER, errorMessage);
			throw new RuntimeException(e);
		} catch(IllegalArgumentException e) {
			this.logError(LOGGER, errorMessage);
			throw new RuntimeException(e);
		} catch(Exception e) {
			this.logError(LOGGER, errorMessage);
			throw new RuntimeException(e);
		} finally {
			inStream.close();
		}
		return results;
	}

	/**
	 * Run SQL queries agains the database from the key-query pair generated
	 * in generateQueries(). Return the update count for logging purposes
	 * @param String key
	 * @param String query
	 * @return int count
	*/
	private int runQuery(String key, String query) throws Exception {
		LOGGER.info("Key: {}", key);
		LOGGER.info("Query: {}", StringUtils.isBlank(query) ? null : query);
		if(StringUtils.isBlank(query)) {
			String errorMessage = "Query for SQL key " + key + " is null or empty";
			this.logError(LOGGER, errorMessage);
			return 0;
		}
		int updateCount = 0;
		try {
			updateCount = this.jdbcTemplate.update(query, new HashMap<String, Object>());
		} catch(DataAccessException e) {
			String errorMessage = "Query for SQL key " + key + " cannot run: \n" + e.getMessage();
			this.logError(LOGGER, errorMessage);
			return 0;
		}
		return updateCount;
	}

	/**
	 * Move XML input file to archive location. Archive location is the same as input location.
	 * The archive folder name is input file name append "_Archive". Archive file name will append
	 * the timestamp.
	 * Safety net: original input file will be deleted after archiving to prevent unintended queries
	 * being executed
	*/
	public void moveToArchive() {
		try {
			Path filePath = Paths.get(this.xmlLocation);
			String fileName = filePath.getFileName().toString();
			Path archiveLocation = this.buildArchiveLocation(filePath, fileName);
			this.createDirectory(archiveLocation);
			Files.copy(filePath, archiveLocation, StandardCopyOption.REPLACE_EXISTING);
			Files.deleteIfExists(filePath);
		} catch(Exception e) {
			String errorMessage = "Error while trying to move input file to archive location. Queries did not run";
			this.logError(LOGGER, errorMessage);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create location: same as input location with an extra folder. Folder name: inputName + "_Archive"
	 * in generateQueries(). Return the update count for logging purposes
	 * @param Path filePath
	 * @param String fileName
	 * @return Path newPath
	*/
	public Path buildArchiveLocation(Path filePath, String fileName) throws Exception {
		int index = fileName.indexOf(".");
		if(index <= 0) {
			throw new Exception("File name is invalid for " + fileName);
		}
		String name = fileName.substring(0, index);
		String type = fileName.substring(index, fileName.length());
		String newFileName = this.buildArchiveName(name, type);
		StringBuilder newFolderName = new StringBuilder();
		newFolderName.append(name);
		newFolderName.append("_Archive");
		return filePath.getParent().resolve(newFolderName.toString()).resolve(newFileName);
	}

	/**
	 * Archive name is the inputName + timestamp up to seconds to avoid duplicates
	 * @param Path filePath
	 * @param String fileName
	 * @return Path newPath
	*/
	public String buildArchiveName(String name, String type) throws Exception {
		LocalDateTime tmStampLdt = LocalDateTime.now();
		Date tmstamp = Date.from(tmStampLdt.atZone(ZoneId.systemDefault()).toInstant());
		SimpleDateFormat formatter = new SimpleDateFormat("_yyyyMMdd_'T'kkmmss");
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(formatter.format(tmstamp));
		sb.append(type);
		return sb.toString();
	}

	/**
	 * Create new directories and throws exception if necessary. Exception handling can be
	 * modified accordingly to follow best practice
	 * @param Path newFile
	*/
	public void createDirectory(Path newFile) throws Exception {
		File file = new File(newFile.getParent().toString());
		if(!file.exists()) {
			if(file.mkdir()) {
				LOGGER.info("New archive directory created: {}", newFile.getParent().toString());
			} else {
				throw new Exception("Archive folder failed to be created");
			}
		}
	}

	/**
	 * Spring data source configuration to SQL database
	 * @param DataSource dataSource
	*/
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		this.jdbcTemplate = new NamedParameterJdbcTemplate(this.dataSource);
	}

	/**
	 * Spring injection of the input file location
	 * @param String xmlLocation
	*/
	public void setXmlLocation(String xmlLocation) {
		this.xmlLocation = xmlLocation;
	}

	/**
	 * Custom comparison needed in order to run queries in order of the key
	*/
	private class KeyComparison implements Comparator<Object> {
		@Override
		public int compare(Object sqlKey1, Object sqlKey2) {
			String key1 = String.valueOf(sqlKey1);
			String key2 = String.valueOf(sqlKey2);
			return Integer.valueOf(key1).compareTo(Integer.valueOf(key2));
		}
	}

}
