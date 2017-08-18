package com.sas.esp.custom.camel.component.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sas.esp.api.dfESPException;
import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientGDStatus;
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;
import com.sas.esp.api.server.datavar;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvDateTime;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvDouble;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvInt32;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvInt64;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvString;
import com.sas.esp.api.server.ReferenceIMPL.dfESPdvTimeStamp;
import com.sas.esp.api.server.ReferenceIMPL.dfESPevent;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;
import com.sas.esp.api.server.datavar.FieldTypes;
import com.sas.esp.api.server.event.EventOpcodes;
import com.sas.esp.api.server.eventblock.EventBlockType;
import com.sas.esp.custom.camel.component.spi.dfESPUuidGenerator;

/**
 * The dfESP producer.
 */
public class dfESPProducer extends DefaultProducer implements clientCallbacks, dfESPAutoRecoverable {

	// Logger
	private static final Logger logger = LoggerFactory.getLogger(dfESPProducer.class);

	// Camel
	private dfESPEndpoint endpoint;

	// URI Params
	private String endpointUri;
	private String espUri;

	private EventOpcodes defaultOpcode;
	private boolean addId;
	private dfESPUuidGenerator uuidGenerator;

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
	private dfESPschema schema;

	private dfESPEndpointState state;

	// Auto Recover
	dfESPAutoRecoverAgent autoRecoverAgent = null;


	public dfESPProducer(dfESPEndpoint endpoint) {
		super((Endpoint) endpoint);
		this.endpoint = endpoint;

		this.state = dfESPEndpointState.STOPPED;

		// set the URIs
		endpointUri = endpoint.getEndpointUri();
		espUri = endpointUri.split("\\?")[0];

		defaultOpcode = endpoint.getProducerDefaultOpcode();
		addId = endpoint.getProducerAddId();

		// UUID Generator

		// First get it from the endpoint
		uuidGenerator = endpoint.getProducerUuidGenerator();

		// if null, try to get from the context
		if (uuidGenerator == null) {
			uuidGenerator = (dfESPUuidGenerator) endpoint.getCamelContext().getRegistry().lookupByNameAndType("dfESPUuidGenerator",
					com.sas.esp.custom.camel.component.spi.dfESPUuidGenerator.class);
		}

		eventAsList = endpoint.getEventAsList();
		bodyType = endpoint.getBodyType();
		eventFieldsAsString = endpoint.getEventFieldsAsString();
		nullString = endpoint.getNullString();

		autoRecover = endpoint.getAutoRecover();
		autoRecoverInterval = endpoint.getAutoRecoverInterval();
		forceStart = endpoint.getForceStart();		

		// if forceStart = true, assume autoRecover=true 
		if (forceStart == true) {
			autoRecover = true;
		}		

		// Create the dfESP client handler
		clientHandler = new dfESPclientHandler();

	}

	@Override
	protected void doStart() throws Exception {

		try {
			// start the subscriber
			startEspPublisher();
		} catch (Exception e) {

			if (forceStart == true) {
				logger.warn("doStart: error starting endpoint [{}] [{}] - forcing start", endpointUri, e.getMessage());
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

		// stop the publisher
		stopEspPublisher();
		
		// stop the auto recover agent
		if (autoRecoverAgent != null) {
			autoRecoverAgent.stop();
			autoRecoverAgent = null;
		}

	}
	
	// Method to start the ESP Publisher
	private void startEspPublisher() throws Exception {

		if (state == dfESPEndpointState.STOPPED) {

			state = dfESPEndpointState.INITIALIZING;

			/*  Initialize publishing capabilities.  This is the first pub/sub API call that must
			 *  be made, and it only needs to be called once.  The parameter is the log level,
			 *  so we are turning logging off for this example.
			 */

			if (!clientHandler.init(Level.WARNING)) {
				state = dfESPEndpointState.STOPPED;
				throw new Exception("startEspPublisher: error in init()");
			}

			state = dfESPEndpointState.INITIALIZED;

		}


		if (state == dfESPEndpointState.INITIALIZED) {

			/* Start this publish session.  This validates publish connection parameters,
			 * but does not make the actual connection.
			 * The parameter for this call is the following URL:
			 * dfESP://host:port/project/contquery/window
			 */

			state = dfESPEndpointState.STARTING;

			client = null;
			try {
				client = clientHandler.publisherStart(espUri, this, 0);
			} catch (Exception e) {
				state = dfESPEndpointState.INITIALIZED;
				throw new Exception("startEspPublisher: error in publisherStart: "  + e.getMessage());
			}

			if (client == null) {
				state = dfESPEndpointState.INITIALIZED;
				throw new Exception("startEspPublisher: error creating client");
			}

			state = dfESPEndpointState.STARTED;

		}


		if (state == dfESPEndpointState.STARTED) {

			state = dfESPEndpointState.PREPARING;

			/* Get the window schema. The URL for this is as follows:
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
				throw new Exception("startEspPublisher: error in schema query");
			}

			logger.info("startEspPublisher: schema is " + schemaVector.get(0));

			try {
				schema = new dfESPschema(schemaVector.get(0));
			} catch (dfESPException e) {
				state = dfESPEndpointState.STARTED;
				throw new Exception("startEspPublisher: error in creating schema");
			}

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
				throw new Exception("startEspPublisher: " + e.getMessage());
			}

			state = dfESPEndpointState.CONNECTED;

		}


	}



	// Method to stop the ESP Publisher
	private void stopEspPublisher() throws Exception {

		/* Stop pubsub, but block (i.e., true) to ensure that all queued events are first processed. */
		
		try {
			if (!clientHandler.stop(client, true)) {
				throw new Exception("error in stop()");
			}
			state = dfESPEndpointState.STOPPED;
		} catch (Exception e) {
			throw new Exception("stopEspPublisher: error stopping pub: " + e.getMessage());
		}

	}


	// Methods needed by the dfESPAutoRecoverAgent
	public void recover() throws Exception {
		startEspPublisher();
	}

	public dfESPEndpoint getEndpoint() {
		return this.endpoint;
	}	



	@SuppressWarnings("unchecked")
	public void process(Exchange exchange) throws Exception {

		if (logger.isDebugEnabled()){
			logger.debug("[{}] exchange [{}] received", espUri, exchange.getExchangeId());
		}

		// the event opcode
		EventOpcodes opcode = null;

		// try to get the opcode from the header
		String opcodeHeader = exchange.getIn().getHeader("dfESP.producerOpcode", String.class);
		if (opcodeHeader != null) {
			try {
				opcode = EventOpcodes.valueOf(opcodeHeader);
			} catch (Exception e) {
				throw new IllegalArgumentException("invalid dfESP.producerOpcode [" + exchange.getIn().getHeader("dfESP.producerOpcode", String.class) + "] found in header");
			}
		}

		// if not found in the header, try to use the default opcode from the endpoint
		// throw exception if not present
		if (opcode == null) {
			if (defaultOpcode != null) {
				opcode = defaultOpcode;
			} else {
				throw new IllegalArgumentException("no dfESP.producerOpcode found in header and no defaultProducerOpcode supplied");
			}
		}

		/* if not connected throw exception */
		if (!(state == dfESPEndpointState.CONNECTED)) {
			throw new IllegalStateException("publisher not connected");
		}


		/* Create array list that will hold the ESP events */
		ArrayList<dfESPevent> espEventList = new ArrayList<dfESPevent>();

		/* Get the message from the exchange */
		Object messageBody = exchange.getIn().getBody();

		// to hold the event list
		List<?> eventList;

		// validate the message body
		try {

			// if the body is an event block
			if (bodyType == dfESPEndpointBodyType.eventBlock) {

				// cast to the appropriate type based on eventAsList
				if (!eventAsList) {
					eventList = (List<Map<String, Object>>) messageBody;
				} else {
					eventList = (List<List<Object>>) messageBody;
				}


				// else we should treat the body as an event
			} else {

				// FIXME should we encapsulate the event processor to not do this?

				// add the event to the list casting to the appropriate type based on eventAsList
				if (!eventAsList) {
					// create a List to hold the event
					eventList = new ArrayList<Map<String, Object>>();
					((ArrayList<Map<String, Object>>) eventList).add(((Map<String, Object>) messageBody));
				} else {
					// create a List to hold the event
					eventList = new ArrayList<List<Object>>();
					((ArrayList<List<Object>>) eventList).add(((List<Object>) messageBody));
				}


			}

		} catch (Exception e) {
			throw new IllegalArgumentException("invalid message body - " + e.getMessage());
		}


		/* Loop through the input event list */
		for (int eventIndx = 0; eventIndx < eventList.size(); eventIndx++) {

			if (logger.isDebugEnabled()){
				logger.debug("[{}] exchange [{}] processing event [{}]", espUri, exchange.getExchangeId(), eventIndx);
			}

			// Get the event
			Object eventFields = eventList.get(eventIndx);

			// Hold the addId field quantity (0 or 1) to make things easier later
			int addIdQty = (addId == true ? 1 : 0);

			// Basic check to see if the event fields are consistent with the schema
			// and if we got a correct representation of the event
			// based on the params
			int fieldsQty = addIdQty;
			try {
				if (!eventAsList) {
					fieldsQty += ((Map<String, Object>) eventFields).size();
				} else {
					fieldsQty += ((List<Object>) eventFields).size();
				}
			} catch (Exception e) {
				throw new IllegalArgumentException("invalid message body - " + e.getMessage());
			}


			if (fieldsQty != schema.getTypes().size()) {
				throw new Exception("process: invalid number of fields, expected " + schema.getNumFields() + ", got " + fieldsQty);
			}


			// Create the list that will hold the ESP types
			ArrayList<datavar> espFieldsList = new ArrayList<datavar>();

			// Add the message id as the first field if applicable
			if (addId == true) {

				datavar uuid;

				// if we don't have a uuid generator, generate the id using the exchange id 
				if (uuidGenerator == null) {
					// If the list has more than one element, append the event number to keep the Id unique
					uuid = new dfESPdvString(exchange.getExchangeId() + (eventList.size() == 1 ? "" : "-" + eventIndx));
				} else {
					// else use the generator
					uuid =  new dfESPdvString(uuidGenerator.generateUuid());
				}

				espFieldsList.add(uuid);
			}


			// Loop through input event fields
			for (int fieldIndx = addIdQty; fieldIndx < schema.getNumFields(); fieldIndx++) {

				// Get the field name, type and value
				String fieldName =  schema.getNames().get(fieldIndx).toString();
				FieldTypes fieldType = schema.getTypes().get(fieldIndx);

				// if map, we should have the field name in the map
				if (!eventAsList && !((Map<String, Object>) eventFields).containsKey(fieldName)) {
					throw new IllegalArgumentException("process: message body does not contain field " + fieldName);
				}


				Object fieldValue;
				if (!eventAsList) {
					fieldValue = ((Map<String, Object>) eventFields).get(fieldName);
				} else {
					fieldValue = ((List<Object>) eventFields).get(fieldIndx - addIdQty);
				}


				// The dfESPdatavar should be null when the field is null or 
				// when eventFieldsAsString = true and the string equals nullString
				boolean isNull = (
						(fieldValue == null) || 
						(
								(eventFieldsAsString == true) &&
								((String) fieldValue).equals(nullString)
								)
						);

				datavar dv = null;

				//TODO: throw a custom exception if the field does not match the required type

				switch (fieldType) {

				case INT32:

					dfESPdvInt32 dvInt32 =  new dfESPdvInt32();
					dv = dvInt32;

					if (!isNull) {
						if (!eventFieldsAsString) {
							dvInt32.setValue((Integer) fieldValue);
						} else {
							dvInt32.setValue(Integer.valueOf((String) fieldValue));
						}
					}

					break;


				case INT64:

					dfESPdvInt64 dvInt64 = new dfESPdvInt64();
					dv = dvInt64;

					if (!isNull) {
						if (!eventFieldsAsString) {
							dvInt64.setValue((Long) fieldValue);
						} else {
							dvInt64.setValue(Long.valueOf((String) fieldValue));
						}
					}

					break;


				case UTF8STR:

					dfESPdvString dvString = new dfESPdvString();
					dv = dvString;

					if (!isNull) {
						dvString.setValue((String) fieldValue);
					}

					break;


				case DATETIME:

					dfESPdvDateTime dvDateTime = new dfESPdvDateTime();
					dv = dvDateTime;

					if (!isNull) {
						if (!eventFieldsAsString) {
							dvDateTime.setValue((Date) fieldValue);
						} else {
							dvDateTime.setValue(endpoint.getDateFormatter().parse((String) fieldValue));
						}
					}

					break;
					

				case TIMESTAMP:

					dfESPdvTimeStamp dvTimeStamp = new dfESPdvTimeStamp();
					dv = dvTimeStamp;

					if (!isNull) {
						if (!eventFieldsAsString) {
							dvTimeStamp.setValue((Date) fieldValue);
						} else {
							dvTimeStamp.setValue(endpoint.getDateFormatter().parse((String) fieldValue));
						}
					}

					break;


				case DOUBLE:

					dfESPdvDouble dvDouble = new dfESPdvDouble();
					dv = dvDouble;

					if (!isNull) {
						if (!eventFieldsAsString) {
							dvDouble.setValue((Double) fieldValue);
						} else {
							dvDouble.setValue(Double.valueOf((String) fieldValue));
						}
					}

					break;


				case MONEY:

					// FIXME 
					throw new Exception ("dfESPsubscriberCB_func: datatype MONEY not currently supported by pubsub API");

					//dfESPdvMoney dvMoney = new dfESPdvMoney();
					//dv = dvMoney;

					//ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
					//ObjectOutputStream objectStream = new ObjectOutputStream(outputStream);
					//objectStream.writeObject((Double) fieldValue);
					//dvMoney.setValue(outputStream.toByteArray());


				default: 
					throw new IllegalArgumentException("process: invalid type " + fieldType.toString());

				}


				// Add the field to the esp field list
				espFieldsList.add(dv);

			}


			/* Create the dfESPevent with the fields and schema */
			dfESPevent event = new dfESPevent(schema, espFieldsList, opcode, 0);


			/* Add the event to the ESP event list */
			espEventList.add(event);

			if (logger.isDebugEnabled()){
				logger.debug("[{}] exchange [{}] queueing event [{}] succeeded", espUri, exchange.getExchangeId(), eventIndx);
			}

		}



		/* Create the dfESPeventblock */
		dfESPeventblock eventBlock = new dfESPeventblock(espEventList, EventBlockType.ebt_NORMAL);

		/* Inject the event block */
		if (!clientHandler.publisherInject(client, eventBlock)) {
			throw new RuntimeException("process: error in publisherInject()");
		}

		if (logger.isDebugEnabled()){
			logger.debug("[{}] exchange [{}] inject block succeeded", espUri, exchange.getExchangeId());
		}

	}

	/* We need to define a dummy subscribe method which is unused by a publisher.
	 */
	public void dfESPsubscriberCB_func(dfESPeventblock eventBlock, dfESPschema schema, Object ctx) {
	}


	/* We need to define a dummy publish method which is only used when implementing
	 * guaranteed delivery.
	 */
	public void dfESPGDpublisherCB_func(clientGDStatus eventBlockStatus, long eventBlockID, Object ctx) {
	}


	/* We also define a callback function for publisher failures given
	 * we may want to try to reconnect/recover, but in this example we will just print
	 * out some error information. The cbf has an optional context pointer for sharing state
	 * across calls or passing state into calls.
	 */
	public void dfESPpubsubErrorCB_func(clientFailures failure, clientFailureCodes code, Object ctx) {

		logger.error("dfESPpubsubErrorCB_func: [{}] publisher failure [{}] with code [{}]", espUri, failure.name(), code.name());

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
					logger.info("dfESPpubsubErrorCB_func: [{}] autoRecover is enabled, setting the publisher state to INITIALIZED.", espUri);
					state = dfESPEndpointState.INITIALIZED;
				}
				autoRecoverAgent.start();
			} else {
				// else fail the component
				logger.error("dfESPpubsubErrorCB_func: [{}] autoRecover is not enabled, shutting down publisher. All subsequent publish attempts will fail. Please restart the component.", espUri);
				clientHandler.shutdown();
				state = dfESPEndpointState.STOPPED;
			}



		}
	}


}
