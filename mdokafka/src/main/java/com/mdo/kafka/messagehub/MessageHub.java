package com.mdo.kafka.messagehub;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

//import com.messagehub.samples.bluemix.BluemixEnvironment;
//import com.messagehub.samples.bluemix.MessageHubCredentials;
//import com.messagehub.samples.rest.RESTAdmin;

/**
 * Console-based sample interacting with Message Hub, authenticating with
 * SASL/PLAIN over an SSL connection.
 *
 * @author IBM
 */
//@Configuration
public class MessageHub {

	private static final String JAAS_CONFIG_PROPERTY = "java.security.auth.login.config";
	private static final String APP_NAME = "ao-stream";
	private static final String DEFAULT_TOPIC_NAME = "mdotopic.t";
	private static final Logger logger = Logger.getLogger(MessageHub.class);

	private static Thread consumerThread = null;
	private static ConsumerRunnable consumerRunnable = null;
	private static ConsumerHub consumerHub = null;
	private static Thread producerThread = null;
	private static ProducerRunnable producerRunnable = null;
	private static ProducerHub producerHub = null;
	private static String resourceDir;

	// add shutdown hooks (intercept CTRL-C etc.)
	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				logger.log(Level.WARN, "Shutdown received.");
				shutdown();
			}
		});
		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				logger.log(Level.ERROR, "Uncaught Exception on " + t.getName()
						+ " : " + e, e);
				shutdown();
			}
		});
	}
	
	@Bean
	public Properties connectBlueMix() {
		final Properties clientProperties = new Properties();
		try {
			final String userDir = System.getProperty("user.dir");
			final boolean isRunningInBluemix = BluemixEnvironment
					.isRunningInBluemix();
			// final boolean isRunningInBluemix = false;

			String bootstrapServers = null;
			String adminRestURL = null;
			String apiKey = null;
			boolean runConsumer = true;
			boolean runProducer = true;
			String topicName = DEFAULT_TOPIC_NAME;

			// Check environment: Bluemix vs Local, to obtain configuration
			// parameters
			if (isRunningInBluemix) {

				logger.log(Level.INFO, "Running in Bluemix mode.");
				resourceDir = userDir + File.separator + APP_NAME
						+ File.separator + "bin" + File.separator + "resources";

				MessageHubCredentials credentials = BluemixEnvironment
						.getMessageHubCredentials();

				bootstrapServers = stringArrayToCSV(credentials
						.getKafkaBrokersSasl());
				adminRestURL = credentials.getKafkaRestUrl();
				apiKey = credentials.getApiKey();

				updateJaasConfiguration(credentials.getUser(),
						credentials.getPassword());
				/*
				 * updateJaasConfiguration("75qmDzPML2dl8RCG",
				 * "n6XCpaVcDZ4YIqE5GGMwOAHKCw02QcNN");
				 */

				/* added for blue mix starts */

				// inject bootstrapServers in configuration, for both consumer
				// and
				// producer
				clientProperties.put("bootstrap.servers", bootstrapServers);

				logger.log(Level.INFO, "Kafka Endpoints: " + bootstrapServers);
				logger.log(Level.INFO, "Admin REST Endpoint: " + adminRestURL);

				// Using Message Hub Admin REST API to create and list topics
				// If the topic already exists, creation will be a no-op
				try {
					logger.log(Level.INFO, "Creating the topic " + topicName);
					String restResponse = RESTAdmin.createTopic(adminRestURL,
							apiKey, topicName);
					logger.log(Level.INFO, "Admin REST response :"
							+ restResponse);

					String topics = RESTAdmin.listTopics(adminRestURL, apiKey);
					logger.log(Level.INFO, "Admin REST Listing Topics: "
							+ topics);
				} catch (Exception e) {
					logger.log(Level.ERROR,
							"Error occurred accessing the Admin REST API " + e,
							e);
					// The application will carry on regardless of Admin REST
					// errors, as the topic may already exist
				}

			} else {

			}

		} catch (Exception e) {
			logger.log(Level.ERROR,
					"Exception occurred, application will terminate", e);
			System.exit(-1);
		}
		clientProperties.put("bootstrap.servers", "kafka05-prod01.messagehub.services.us-south.bluemix.net:9093");
		return clientProperties;
	}
	
	@Bean
	public ProducerHub producerHub() {

		String topicName = DEFAULT_TOPIC_NAME;
		Properties connectBlueMix = connectBlueMix();
		//Properties connectBlueMix = new Properties();
		//connectBlueMix.put("bootstrap.servers", "kafka05-prod01.messagehub.services.us-south.bluemix.net:9093");

		Properties producerProperties = getClientConfiguration(connectBlueMix,
				"producer.properties");
		producerHub = new ProducerHub(producerProperties, topicName);
		return producerHub;
	}
	
	//@Bean
	public ConsumerHub consumerHub() {

		String topicName = DEFAULT_TOPIC_NAME;
		Properties connectBlueMix = connectBlueMix();
		
	
		Properties consumerProperties = getClientConfiguration(connectBlueMix,
				"consumer.properties");
		consumerHub = new ConsumerHub(consumerProperties, topicName);
		return consumerHub;
	}

	// public void connectTopic() {
	static {
		try {
			final String userDir = System.getProperty("user.dir");
			final boolean isRunningInBluemix = BluemixEnvironment
					.isRunningInBluemix();
			// final boolean isRunningInBluemix = false;
			final Properties clientProperties = new Properties();

			String bootstrapServers = null;
			String adminRestURL = null;
			String apiKey = null;
			boolean runConsumer = true;
			boolean runProducer = true;
			String topicName = DEFAULT_TOPIC_NAME;

			// Check environment: Bluemix vs Local, to obtain configuration
			// parameters
			if (isRunningInBluemix) {

				logger.log(Level.INFO, "Running in Bluemix mode.");
				resourceDir = userDir + File.separator + APP_NAME
						+ File.separator + "bin" + File.separator + "resources";

				MessageHubCredentials credentials = BluemixEnvironment
						.getMessageHubCredentials();

				bootstrapServers = stringArrayToCSV(credentials
						.getKafkaBrokersSasl());
				adminRestURL = credentials.getKafkaRestUrl();
				apiKey = credentials.getApiKey();

				// bootstrapServers =
				// "kafka05-prod01.messagehub.services.us-south.bluemix.net:9093";
				// adminRestURL =
				// "https://kafka-rest-prod01.messagehub.services.us-south.bluemix.net:443";
				// apiKey = "75qmDzPML2dl8RCGn6XCpaVcDZ4YIqE5GGMwOAHKCw02QcNN";

				updateJaasConfiguration(credentials.getUser(),
						credentials.getPassword());
				/*
				 * updateJaasConfiguration("75qmDzPML2dl8RCG",
				 * "n6XCpaVcDZ4YIqE5GGMwOAHKCw02QcNN");
				 */

				/* added for blue mix starts */

				// inject bootstrapServers in configuration, for both consumer
				// and
				// producer
				clientProperties.put("bootstrap.servers", bootstrapServers);

				logger.log(Level.INFO, "Kafka Endpoints: " + bootstrapServers);
				logger.log(Level.INFO, "Admin REST Endpoint: " + adminRestURL);

				// Using Message Hub Admin REST API to create and list topics
				// If the topic already exists, creation will be a no-op
				try {
					logger.log(Level.INFO, "Creating the topic " + topicName);
					String restResponse = RESTAdmin.createTopic(adminRestURL,
							apiKey, topicName);
					logger.log(Level.INFO, "Admin REST response :"
							+ restResponse);

					String topics = RESTAdmin.listTopics(adminRestURL, apiKey);
					logger.log(Level.INFO, "Admin REST Listing Topics: "
							+ topics);
				} catch (Exception e) {
					logger.log(Level.ERROR,
							"Error occurred accessing the Admin REST API " + e,
							e);
					// The application will carry on regardless of Admin REST
					// errors, as the topic may already exist
				}

				// create the Kafka clients

				// modified for AO

				/*
				 * if (runConsumer) { Properties consumerProperties =
				 * getClientConfiguration( clientProperties,
				 * "consumer.properties"); consumerRunnable = new
				 * ConsumerRunnable(consumerProperties, topicName);
				 * consumerThread = new Thread(consumerRunnable,
				 * "Consumer Thread"); consumerThread.start(); }
				 */

				Properties consumerProperties = getClientConfiguration(
						clientProperties, "consumer.properties");
				consumerHub = new ConsumerHub(consumerProperties, topicName);

				// modified for AO

				/*
				 * if (runProducer) { Properties producerProperties =
				 * getClientConfiguration( clientProperties,
				 * "producer.properties"); producerRunnable = new
				 * ProducerRunnable(producerProperties, topicName);
				 * producerThread = new Thread(producerRunnable,
				 * "Producer Thread"); producerThread.start(); }
				 */

				Properties producerProperties = getClientConfiguration(
						clientProperties, "producer.properties");
				producerHub = new ProducerHub(producerProperties, topicName);

				logger.log(Level.INFO, "MessageHub will run until interrupted.");

				/* added for blue mix ends */

			} else {

			}

		} catch (Exception e) {
			logger.log(Level.ERROR,
					"Exception occurred, application will terminate", e);
			System.exit(-1);
		}
	}

	/*
	 * convenience method for cleanup on shutdown
	 */
	private static void shutdown() {
		if (producerRunnable != null)
			producerRunnable.shutdown();
		if (consumerRunnable != null)
			consumerRunnable.shutdown();
		if (producerThread != null)
			producerThread.interrupt();
		if (consumerThread != null)
			consumerThread.interrupt();
	}

	/*
	 * Return a CSV-String from a String array
	 */
	private static String stringArrayToCSV(String[] sArray) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < sArray.length; i++) {
			sb.append(sArray[i]);
			if (i < sArray.length - 1)
				sb.append(",");
		}
		return sb.toString();
	}

	/*
	 * Retrieve client configuration information, using a properties file, for
	 * connecting to Message Hub Kafka.
	 */
	static final Properties getClientConfiguration(Properties commonProps,
			String fileName) {
		Properties result = new Properties();
		InputStream propsStream;

		try {
			propsStream = new FileInputStream(resourceDir + File.separator
					+ fileName);
			result.load(propsStream);
			propsStream.close();
		} catch (IOException e) {
			logger.log(Level.ERROR, "Could not load properties from file");
			return result;
		}

		result.putAll(commonProps);
		return result;
	}

	/*
	 * Updates JAAS config file with provided credentials.
	 */
	private static void updateJaasConfiguration(String username, String password)
			throws IOException {
		// Set JAAS configuration property.
		String jaasConfPath = System.getProperty("java.io.tmpdir")
				+ File.separator + "jaas.conf";
		System.setProperty(JAAS_CONFIG_PROPERTY, jaasConfPath);

		String templatePath = resourceDir + File.separator
				+ "jaas.conf.template";
		OutputStream jaasOutStream = null;

		logger.log(Level.INFO, "Updating JAAS configuration");

		try {
			String templateContents = new String(Files.readAllBytes(Paths
					.get(templatePath)));
			jaasOutStream = new FileOutputStream(jaasConfPath, false);

			// Replace username and password in template and write
			// to jaas.conf in resources directory.
			String fileContents = templateContents.replace("$USERNAME",
					username).replace("$PASSWORD", password);

			jaasOutStream
					.write(fileContents.getBytes(Charset.forName("UTF-8")));
		} catch (final IOException e) {
			logger.log(Level.ERROR, "Failed accessing to JAAS config file", e);
			throw e;
		} finally {
			if (jaasOutStream != null) {
				try {
					jaasOutStream.close();
				} catch (final Exception e) {
					logger.log(Level.WARN,
							"Error closing generated JAAS config file", e);
				}
			}
		}
	}
}