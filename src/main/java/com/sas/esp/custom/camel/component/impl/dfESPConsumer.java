package com.sas.esp.custom.camel.component.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.esp.api.server.datavar;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvDateTime;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvDouble;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvInt32;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvInt64;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvMoney;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvString;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvTimeStamp;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPevent;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;
import com.sas.esp.api.server.event.EventFlags;
import com.sas.esp.api.server.event.EventOpcodes;
import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientGDStatus;
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;
import com.sas.esp.api.dfESPException;




/**
 * The dfESP consumer.
 */
public class dfESPConsumer extends DefaultConsumer implements clientCallbacks, dfESPAutoRecoverable{

	// Logger
	private static final Logger logger = LoggerFactory.getLogger(dfESPConsumer.class);

	// Camel
	private final dfESPEndpoint endpoint;

	// URI Params
	private String endpointUri;
	private String espUri;


	private EventOpcodes opcodeFilter;
	private boolean snapshot;
	private String flagsFilter;

	private boolean eventAsList;
	private dfESPEndpointBodyType bodyType;
	private boolean eventFieldsAsString;
	private String nullString;

	private boolean autoRecover;
	private long autoRecoverInterval;
	private boolean forceStart;

	// ESP
	private dfESPclientHandler clientHandler;
	private dfESPclient client;
	private ArrayList<String> schemaVector;
	//private dfESPschema schema;

	private dfESPEndpointState state;

	// Auto Recover
	dfESPAutoRecoverAgent autoRecoverAgent = null;


	public dfESPConsumer(dfESPEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;

		this.state = dfESPEndpointState.STOPPED;

		// set the URIs
		endpointUri = endpoint.getEndpointUri();
		espUri = endpointUri.split("\\?")[0];


		// set the params from the endpoint

		opcodeFilter = endpoint.getConsumerOpcodeFilter();
		snapshot = endpoint.getConsumerSnapshot();
		flagsFilter = endpoint.getConsumerFlagsFilter();

		eventAsList = endpoint.getEventAsList();
		bodyType = endpoint.getBodyType();
		eventFieldsAsString = endpoint.getEventFieldsAsString();
		nullString = endpoint.getNullString();

		autoRecover = endpoint.getAutoRecover();
		autoRecoverInterval = endpoint.getAutoRecoverInterval();
		forceStart = endpoint.getForceStart();


		// if forceStart = true, assume autoRecover=true 
		// we could wait for the callback to do this but the agent will keep only one recovery thread
		// so we can start it here as well		
		if (forceStart == true) {
			autoRecover = true;
		}

		// create the dfESP client handler
		clientHandler = new dfESPclientHandler();

	}


	@Override
	protected void doStart() throws Exception {

		try {
			// start the subscriber
			startEspSubscriber();
		} catch (Exception e) {

			if (forceStart == true) {
				logger.warn("doStart: error starting endpoint [{}] - [{}] - forcing start", endpointUri, e.getMessage());
			} else {
				throw e;
			}

		}


		// create the auto recover agent if applicable
		if (autoRecover == true) {
			autoRecoverAgent = new dfESPAutoRecoverAgent(this, autoRecoverInterval);
		}

		// if forceStart = true and failed to start, start the auto recover agent
		// we could wait for the callback to do this but the agent will keep only one recovery thread
		// so we can start it here as well		
		if (forceStart == true && state != dfESPEndpointState.CONNECTED) {
			// start the agent if necessary
			autoRecoverAgent.start();
		}

	}


	@Override
	protected void doStop() throws Exception {

		// stop the subscriber
		stopEspSubscriber();

		// stop the auto recover agent
		if (autoRecoverAgent != null) {
			autoRecoverAgent.stop();
			autoRecoverAgent = null;
		}

	}



	// Method to start the ESP Subscriber
	private void startEspSubscriber() throws Exception {


		if (state == dfESPEndpointState.STOPPED) {

			state = dfESPEndpointState.INITIALIZING;

			/*  Initialize subscribing capabilities.  This is the first pub/sub API call that must
			 *  be made, and it only needs to be called once. The parameter is the log level,
			 *  so we are turning logging off for this example.
			 */

			if (!clientHandler.init(Level.WARNING)) {
				state = dfESPEndpointState.STOPPED;
				throw new Exception("startEspSubscriber: error in init()");
			}

			state = dfESPEndpointState.INITIALIZED;

		}


		if (state == dfESPEndpointState.INITIALIZED) {

			/* Start this subscribe session.  This validates subscribe connection parameters, 
			 * but does not make the actual connection.
			 * The parameters for this call are URL, user defined subscriber callback function,
			 * an optional user defined subscribe services error callback function, and an
			 * optional user defined context pointer (NULL in this case).
			 * The URL is as follows 
			 * dfESP://host:port/project/contquery/window?snapshot=true/false
			 * When a new window event arrives, the callback function is invoked to process it.
			 */

			state = dfESPEndpointState.STARTING;

			client = null;
			try {
				client = clientHandler.subscriberStart(espUri + "?snapshot=" + snapshot, this, 0);
			} catch (Exception e) {
				state = dfESPEndpointState.INITIALIZED;
				throw new Exception("startEspPublisher: error in subscriberStart: " + e.getMessage());
			}

			if (client == null) {
				state = dfESPEndpointState.INITIALIZED;
				throw new Exception("startEspPublisher: error creating client");
			}

			state = dfESPEndpointState.STARTED;

		}		


		if (state == dfESPEndpointState.STARTED) {

			state = dfESPEndpointState.PREPARING;

			/* Get the schema and write it to the log.
			 * The URL is as follows:
			 * dfESP://host:port/project/contquery/window?get=schema
			 */
			String schemaUrl = espUri + "?get=schema";

			schemaVector = null;
			try {
				schemaVector = clientHandler.queryMeta(schemaUrl);
			} catch (Exception e) {
				state = dfESPEndpointState.STARTED;
				throw new Exception("startEspPublisher: error in queryMeta()");
			}

			if (schemaVector == null) {
				state = dfESPEndpointState.STARTED;
				throw new Exception("startEspSubscriber: error in schema query");
			}


			logger.info("startEspSubscriber: schema is " + schemaVector.get(0));

			state = dfESPEndpointState.PREPARED;

		}


		if (state == dfESPEndpointState.PREPARED) {

			/* Now make the actual connection to the ESP application or server. */
			try {

				state = dfESPEndpointState.CONNECTING;

				if (!clientHandler.connect(client)) {
					throw new Exception("error in connect()");
				}

			} catch (Exception e) {
				state = dfESPEndpointState.PREPARED;
				throw new Exception("startEspSubscriber: " + e.getMessage());
			}

			state = dfESPEndpointState.CONNECTED;

		}


	}

	// Method to stop the ESP Subscriber
	private void stopEspSubscriber() throws Exception {

		/* Stop pubsub, but block (i.e., true) to ensure that all queued events are first processed. */
		logger.info("stopEspSubscriber: stopping consumer");
		clientHandler.stop(client, true);

	}


	// Methods needed by the dfESPAutoRecoverAgent
	public void recover() throws Exception {
		startEspSubscriber();
	}

	public dfESPEndpoint getEndpoint() {
		return this.endpoint;
	}



	/* We need to define a subscribe method which will get called when new events
	 * are published from the server via the pub/sub API. This method gets an
	 * eventblock, the schema of the event block for processing purposes, and an optional
	 * user context pointer supplied by the call to start().
	 */
	public void dfESPsubscriberCB_func(dfESPeventblock eventBlock, dfESPschema schema, Object ctx) {

		// The exchange to output the message
		Exchange exchange = null;

		dfESPevent event;
		int eventCnt = eventBlock.getSize();
		if (logger.isDebugEnabled()) {
			logger.debug("dfESPsubscriberCB_func: [{}] eventBlock [{}] received with [{}] events", espUri, eventBlock.getTID(), eventBlock.getSize());
		}
		// create a list for the events if applicable
		List<Object> eventsList = null;
		if (bodyType == dfESPEndpointBodyType.eventBlock) {
			eventsList = new ArrayList<Object>();
		}


		for (int eventIndx = 0; eventIndx < eventCnt; eventIndx++) {

			int eventIndxAux = eventIndx;

			/* Get the event out of the event block. */
			event = eventBlock.getEvent(eventIndx);
			
			/* Calculate the event flags */
			String eventFlags = "";

			boolean flagNormal = (event.getFlags() & EventFlags.ef_NORMAL.value) == EventFlags.ef_NORMAL.value;
			if (flagNormal) eventFlags += "N";
			
			boolean flagPartialUpdate = (event.getFlags() & EventFlags.ef_PARTIALUPDATE.value) == EventFlags.ef_PARTIALUPDATE.value;
			if (flagPartialUpdate) eventFlags += "P";			
			
			boolean flagRetention = (event.getFlags() & EventFlags.ef_RETENTION.value) == EventFlags.ef_RETENTION.value;
			if (flagRetention) eventFlags += "R";

			// Only process the event if no opcode filter was supplied or if the opcode filter matches the event opcode and
			//                        if no flags filter was supplied or if the flags filter matches the event flags
			if ((opcodeFilter == null || event.getOpcode().equals(opcodeFilter)) && (flagsFilter == null || eventFlags.equals(flagsFilter))) {


				if (!logger.isTraceEnabled()) {
					if (logger.isDebugEnabled()){
						logger.debug("dfESPsubscriberCB_func: [{}] eventBlock [{}] processing event [{}]", espUri, eventBlock.getTID(), eventIndx);
					}
				} else {

					String eventCsv = "";

					try {
						eventCsv = event.toStringCSV(schema, true, false);
					} catch (dfESPException e) {	

					}

					logger.trace("dfESPsubscriberCB_func: [{}] eventBlock [{}] processing event [{}][{}]", espUri, eventBlock.getTID(), eventIndx, eventCsv);
				}

				try {

					Map<String, Object> fieldsMap = null;
					List<Object> fieldsList = null;
					if (!eventAsList) {
						fieldsMap = new LinkedHashMap<String, Object>();
					} else {
						fieldsList = new ArrayList<Object>();
					}
					// Create a map to hold the result


					// Loop through event fields
					for (int fieldIndx = 0; fieldIndx < schema.getNumFields(); fieldIndx++) {

						// Get the field name and value
						String fieldName =  schema.getNames().get(fieldIndx).toString();
						datavar fieldValue = event.copyByExtID(schema, fieldIndx); 

						// Output the field with the correct type, and also check for null if outputting as string
						if (fieldValue instanceof dfESPdvInt32) {

							dfESPdvInt32 dvInt32 = (dfESPdvInt32) fieldValue;

							if (!eventFieldsAsString) {
								addFieldToOutput(fieldName, dvInt32.getValue(), fieldsList, fieldsMap, eventAsList);	
							} else {

								if (!dvInt32.isNull()) {
									addFieldToOutput(fieldName, dvInt32.getValue().toString(), fieldsList, fieldsMap, eventAsList);
								} else {
									addFieldToOutput(fieldName, nullString, fieldsList, fieldsMap, eventAsList);
								}

							}


						} else if (fieldValue instanceof dfESPdvInt64) {

							dfESPdvInt64 dvInt64 = (dfESPdvInt64) fieldValue;

							if (!eventFieldsAsString) {
								addFieldToOutput(fieldName, dvInt64.getValue(), fieldsList, fieldsMap, eventAsList);	
							} else {

								if (!dvInt64.isNull()) {
									addFieldToOutput(fieldName, dvInt64.getValue().toString(), fieldsList, fieldsMap, eventAsList);
								} else {
									addFieldToOutput(fieldName, nullString, fieldsList, fieldsMap, eventAsList);
								}

							}							


						} else if (fieldValue instanceof dfESPdvString) {

							dfESPdvString dvString = (dfESPdvString) fieldValue;

							if (!eventFieldsAsString) {
								addFieldToOutput(fieldName, dvString.getValue(), fieldsList, fieldsMap, eventAsList);	
							} else {

								if (!dvString.isNull()) {
									addFieldToOutput(fieldName, dvString.getValue(), fieldsList, fieldsMap, eventAsList);
								} else {
									addFieldToOutput(fieldName, nullString, fieldsList, fieldsMap, eventAsList);
								}

							}


						} else if (fieldValue instanceof dfESPdvDateTime) {

							dfESPdvDateTime dvDateTime = (dfESPdvDateTime) fieldValue;

							if (!eventFieldsAsString) {
								addFieldToOutput(fieldName, dvDateTime.getValue(), fieldsList, fieldsMap, eventAsList);	
							} else {

								if (!dvDateTime.isNull()) {
									addFieldToOutput(fieldName, endpoint.getDateFormatter().format(dvDateTime.getValue()), fieldsList, fieldsMap, eventAsList);
								} else {
									addFieldToOutput(fieldName, nullString, fieldsList, fieldsMap, eventAsList);
								}

							}


						} else if (fieldValue instanceof dfESPdvTimeStamp) {

							dfESPdvTimeStamp dvTimeStamp = (dfESPdvTimeStamp) fieldValue;

							if (!eventFieldsAsString) {
								addFieldToOutput(fieldName, dvTimeStamp.getValue(), fieldsList, fieldsMap, eventAsList);
							} else {

								if (!dvTimeStamp.isNull()) {
									addFieldToOutput(fieldName, endpoint.getDateFormatter().format(dvTimeStamp.getValue()), fieldsList, fieldsMap, eventAsList);
								} else {
									addFieldToOutput(fieldName, nullString, fieldsList, fieldsMap, eventAsList);
								}

							}


						} else if (fieldValue instanceof dfESPdvDouble) {

							dfESPdvDouble dvDouble = (dfESPdvDouble) fieldValue;

							if (!eventFieldsAsString) {
								addFieldToOutput(fieldName, dvDouble.getValue(), fieldsList, fieldsMap, eventAsList);	
							} else {

								if (!dvDouble.isNull()) {
									addFieldToOutput(fieldName, dvDouble.getValue().toString(), fieldsList, fieldsMap, eventAsList);
								} else {
									addFieldToOutput(fieldName, nullString, fieldsList, fieldsMap, eventAsList);
								}

							}



						} else if (fieldValue instanceof dfESPdvMoney) {
							// FIXME
							throw new Exception ("dfESPsubscriberCB_func: datatype MONEY not currently supported by pubsub API");

						} else {
							throw new Exception("dfESPsubscriberCB_func: invalid type " + schema.getTypes().get(fieldIndx).toString());
						}

					}


					// create an exchange for each event if applicable
					if (bodyType == dfESPEndpointBodyType.event) {

						// create a new exchange
						exchange = endpoint.createExchange();
						exchange.setPattern(ExchangePattern.InOnly);

						// set the headers
						Map<String, Object> headersMap = new LinkedHashMap<String, Object>();
						headersMap.put("dfESP.eventBlockTid", eventBlock.getTID());
						headersMap.put("dfESP.eventBlockMessageId", eventBlock.getMessageID());
						headersMap.put("dfESP.eventBlockQty", eventCnt);

						// event specific headers
						headersMap.put("dfESP.eventOpcode", event.getOpcode());
						headersMap.put("dfESP.eventFlags", eventFlags);
						headersMap.put("dfESP.eventSeq", eventIndx);

						exchange.getIn().setHeaders(headersMap);							

						// set the exchange body
						if (!eventAsList) {
							exchange.getIn().setBody(fieldsMap);
						} else {
							exchange.getIn().setBody(fieldsList);
						}

						// send message to next processor in the route
						try {

							if (logger.isDebugEnabled()) {
								logger.debug("dfESPsubscriberCB_func: [{}] eventBlock [{}] event [{}] start camel processing", espUri, eventBlock.getTID(), eventIndxAux);
							}

							// super.getProcessor().process(exchange);
							super.getAsyncProcessor().process(exchange, 


									new AsyncCallback() {

								public void done(boolean doneSync) {
									// noop
									if (logger.isDebugEnabled()) {
										logger.debug("dfESPsubscriberCB_func: [{}] eventBlock [{}] event [{}] done camel processing {}", espUri, eventBlock.getTID(), eventIndxAux, doneSync ? "synchronously" : "asynchronously");
									}
								}

							}
									);

						} catch (Exception e) {
							exchange.setException(e);
							// log exception if an exception occurred and was not handled
							if (exchange.getException() != null) {
								getExceptionHandler().handleException("Error processing exchange", exchange, exchange.getException());
							}
						}

					} else {

						// else we have to output the entire event block as a list
						// of Map or list, depending on useList param
						if (!eventAsList) {
							eventsList.add(fieldsMap);
						} else {
							eventsList.add(fieldsList);
						}

						if (logger.isDebugEnabled()) {
							logger.debug("dfESPsubscriberCB_func: [{}] eventBlock [{}] event [{}] dequeueing succeeded", espUri, eventBlock.getTID(), eventIndx);
						}

					}


				} catch (dfESPException e) {
					logger.error("dfESPsubscriberCB_func: [{}] eventBlock [{}] processing event [{}] failed with dfESPException [{}] [{}]", espUri, eventBlock.getTID(), eventIndx, e.getMessage(), e.getStackTrace());
				} catch (Exception e) {
					logger.error("dfESPsubscriberCB_func: [{}] eventBlock [{}] processing event [{}] failed with Exception [{}] [{}]", espUri, eventBlock.getTID(), eventIndx, e.getMessage(), e.getStackTrace());
				}

			} else {
				if (logger.isTraceEnabled()) {
					
					String reason = "";
					
					if (opcodeFilter != null) {
						reason += "opcodeFilter not match -> " + event.getOpcode() + " != " + opcodeFilter; 
					}

					if (flagsFilter != null) {
						if (reason.length() > 0) reason += ","; 
						reason += "flagsFilter not match -> " + eventFlags + " != " + flagsFilter; 
					}					
					
					logger.trace("dfESPsubscriberCB_func: [{}] eventBlock [{}] skipped event [{}] because [{}]", espUri, eventBlock.getTID(), eventIndx, reason);
				}
			}

		}


		// output the event list if applicable
		if (bodyType == dfESPEndpointBodyType.eventBlock) {

			// create a new exchange
			exchange = endpoint.createExchange();
			exchange.setPattern(ExchangePattern.InOnly);

			// set the headers
			Map<String, Object> headersMap = new LinkedHashMap<String, Object>();
			headersMap.put("dfESP.eventBlockTid", eventBlock.getTID());
			headersMap.put("dfESP.eventBlockMessageId", eventBlock.getMessageID());
			headersMap.put("dfESP.eventBlockQty", eventCnt);

			exchange.getIn().setHeaders(headersMap);				

			// set the exchange body
			exchange.getIn().setBody(eventsList);

			// send message to next processor in the route
			try {

				if (logger.isDebugEnabled()) {
					logger.debug("dfESPsubscriberCB_func: [{}] eventBlock [{}] start camel processing", espUri, eventBlock.getTID());
				}

				// process the exchange async
				super.getAsyncProcessor().process(exchange, 


						new AsyncCallback() {
					public void done(boolean doneSync) {
						// noop
						if (logger.isDebugEnabled()) {
							logger.debug("dfESPsubscriberCB_func: [{}] eventBlock [{}] done camel processing {}", espUri, eventBlock.getTID(), doneSync ? "synchronously" : "asynchronously");
						}
					}
				}
						);

				if (logger.isDebugEnabled()){
					logger.debug("dfESPsubscriberCB_func: [{}] eventBlock [{}] processing succeeded", espUri, eventBlock.getTID());
				}
			} catch (Exception e) {
				exchange.setException(e);
				// log exception if an exception occurred and was not handled
				if (exchange.getException() != null) {
					getExceptionHandler().handleException("Error processing exchange", exchange, exchange.getException());
				}
			}

		}

	}



	// Helper method to decide if we will use the list or map for the event fields
	private void addFieldToOutput(String fieldName, Object field, List<Object> fieldsList, Map<String, Object> fieldsMap, boolean useList) {

		if (!useList) {
			fieldsMap.put(fieldName, field);
		} else {
			fieldsList.add(field);
		}

	}



	/*  We also define a callback function for subscription failures given
	 *  we may want to try to reconnect/recover, but in this example we will just print
	 *  out some error information and release the non-busy wait set below so the 
	 *  main in program can end. The cbf has an optional context pointer for sharing state
	 *  across calls or passing state into calls.
	 */
	public void dfESPpubsubErrorCB_func(clientFailures failure, clientFailureCodes code, Object ctx) {

		logger.error("dfESPpubsubErrorCB_func: [{}] subscriber failure [{}] with code [{}]", espUri, failure.name(), code.name());

		// if we have a disconnect, check if autoRecover is enabled
		if (failure == clientFailures.pubsubFail_SERVERDISCONNECT ||
			failure == clientFailures.pubsubFail_APIFAIL && 
			   (code == clientFailureCodes.pubsubCode_WRITEFAILED || 
				code == clientFailureCodes.pubsubCode_READFAILED ||
				code == clientFailureCodes.pubsubCode_READEVENTSIZE ||
				code == clientFailureCodes.pubsubCode_READEVENTS ||
				code == clientFailureCodes.pubsubCode_WRITEEVENTS)
				) {

			if (autoRecover == true) {
				// if enabled, start the auto recover agent
				if (state != dfESPEndpointState.INITIALIZED) {
					logger.info("dfESPpubsubErrorCB_func: [{}] autoRecover is enabled, setting the subscriber state to INITIALIZED.", espUri);
					state = dfESPEndpointState.INITIALIZED;
				}
				autoRecoverAgent.start();
			} else {
				// else fail the component
				logger.error("dfESPpubsubErrorCB_func: [{}] autoRecover is NOT enabled, shutting down the subscriber. Please restart the component.", espUri);
				clientHandler.shutdown();
				state = dfESPEndpointState.STOPPED;
			}

		}

	}


	/* We need to define a dummy publish method which is only used when implementing
	   guaranteed delivery.
	 */
	public void dfESPGDpublisherCB_func(clientGDStatus eventBlockStatus, long eventBlockID, Object ctx) {
	}



}
