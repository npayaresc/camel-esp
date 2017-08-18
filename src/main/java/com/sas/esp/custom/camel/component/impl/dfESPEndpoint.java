package com.sas.esp.custom.camel.component.impl;

import java.text.SimpleDateFormat;

import org.apache.camel.Consumer;
import org.apache.camel.MultipleConsumersSupport;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;

import com.sas.esp.api.server.event.EventOpcodes;
import com.sas.esp.custom.camel.component.spi.dfESPUuidGenerator;

/**
 * Represents a dfESP endpoint.
 */

@ManagedResource(description = "Managed dfESPEndpoint")
@UriEndpoint(scheme = "dfESP", consumerClass = dfESPConsumer.class, syntax = "dfESP://project/contquery/window[?params]", title = "dfESP Endpoint")
public class dfESPEndpoint extends DefaultEndpoint implements MultipleConsumersSupport {
	
	
	// Thread Local SimpleDateFormat
	private ThreadLocalSimpleDateFormat tlSdf = null;
	
	
	// consumer specific params
	@UriParam
	private EventOpcodes consumerOpcodeFilter = null;

	@UriParam
	private boolean consumerSnapshot = false;

	@UriParam
	private String consumerFlagsFilter = null;
	
	
	// producer specific params
	@UriParam
	private EventOpcodes producerDefaultOpcode = null;
	
	@UriParam
	private boolean producerAddId = false;
	
	@UriParam
	private dfESPUuidGenerator producerUuidGenerator = null;
	
	
	// general params (event related)
	@UriParam
	private dfESPEndpointBodyType bodyType = dfESPEndpointBodyType.eventBlock;
	
	@UriParam
	private boolean eventAsList = false;
	
	@UriParam
	private boolean eventFieldsAsString = false;
	
	@UriParam
	private String nullString = "";
	
	@UriParam
	private String dateFormat = "yyyy-MM-dd HH:mm:ss";
	
	@UriParam
	private String dateTimezone = null;
		
	
	// general params (auto recover related)
	@UriParam
	private boolean autoRecover = false;
	
	@UriParam
	private long autoRecoverInterval = 60000;
	
	@UriParam
	private boolean forceStart = false;
	
	
	// constructors
    public dfESPEndpoint() {
    }

    public dfESPEndpoint(String uri, dfESPComponent component) {
        super(uri, component);
    }
	
    // factory methods
    public Producer createProducer() throws Exception {
        return new dfESPProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return new dfESPConsumer(this, processor);
    }

    // Camel    
    public boolean isSingleton() {
        return true;
    }
    
	@Override
	public boolean isMultipleConsumersSupported() {
		return true;
	}	
	
	
	// Getter for the SimpleDateFormat
	protected SimpleDateFormat getDateFormatter() {
		
		if (tlSdf == null) {
			tlSdf = new ThreadLocalSimpleDateFormat(dateFormat, dateTimezone);
		}
		
		return tlSdf.getDateFormatter();
	}
    
	
    // Params
    @ManagedAttribute(description = "the event opcode the consume will filter when subscribing")
	public void setConsumerOpcodeFilter(EventOpcodes consumerOpcodeFilter){
		this.consumerOpcodeFilter = consumerOpcodeFilter;
	}

    @ManagedAttribute(description = "the event opcode the consume will filter when subscribing")
	public EventOpcodes getConsumerOpcodeFilter(){
		return this.consumerOpcodeFilter;
	}
	
    
    @ManagedAttribute(description = "if the consumer should get an initial snapshot of the data")
	public void setConsumerSnapshot(boolean consumerSnapshot){
		this.consumerSnapshot = consumerSnapshot;
	}

    @ManagedAttribute(description = "if the consumer should get an initial snapshot of the data")
	public boolean getConsumerSnapshot(){
		return this.consumerSnapshot;
	}

    @ManagedAttribute(description = "the event flags the consumer will filter when subscribing")
	public void setConsumerFlagsFilter(String consumerFlagsFilter){
		this.consumerFlagsFilter = consumerFlagsFilter;
	}

    @ManagedAttribute(description = "the event flags the consumer will filter when subscribing")
	public String getConsumerFlagsFilter(){
		return this.consumerFlagsFilter;
	}    
    

    @ManagedAttribute(description = "the default event opcode the producer will use when publishing, if not specified in message header")
	public void setProducerDefaultOpcode(EventOpcodes producerDefaultOpcode){
		this.producerDefaultOpcode = producerDefaultOpcode;
	}

    @ManagedAttribute(description = "the default event opcode the producer will use when publishing, if not specified in message header")
	public EventOpcodes getProducerDefaultOpcode(){
		return this.producerDefaultOpcode;
	}	


    @ManagedAttribute(description = "if the producer should add an UUID as the first field into the event")
	public void setProducerAddId(boolean producerAddId){
		this.producerAddId = producerAddId;
	}

    @ManagedAttribute(description = "if the producer should add an UUID as the first field into the event")
	public boolean getProducerAddId(){
		return this.producerAddId;
	}	
	

    @ManagedAttribute(description = "the custom UUID Generator for the producer used when producerAddId = true")
	public void setProducerUuidGenerator(dfESPUuidGenerator producerUuidGenerator){
		this.producerUuidGenerator = producerUuidGenerator;
	}

    @ManagedAttribute(description = "the custom UUID Generator for the producer used when producerAddId = true")
	public dfESPUuidGenerator getProducerUuidGenerator(){
		return this.producerUuidGenerator;
	}	
		
	
    
    
    @ManagedAttribute(description = "if the endpoint should use a List instead of a Map for the event fields")
	public void setEventAsList(boolean eventAsList){
		this.eventAsList = eventAsList;
	}

    @ManagedAttribute(description = "if the endpoint should use a List instead of a Map for the event fields")
	public boolean getEventAsList(){
		return this.eventAsList;
	}
	
	
    @ManagedAttribute(description = "which type to use on Camel exchange message body")
	public void setBodyType(dfESPEndpointBodyType bodyType){
		this.bodyType = bodyType;
	}

    @ManagedAttribute(description = "which type to use on Camel exchange message body")
	public dfESPEndpointBodyType getBodyType(){
		return this.bodyType;
	}
	
	
    @ManagedAttribute(description = "if all the event fields should be treated as Strings")
	public void setEventFieldsAsString(boolean eventFieldsAsString){
		this.eventFieldsAsString = eventFieldsAsString;
	}

    @ManagedAttribute(description = "if all the event fields should be treated as Strings")
	public boolean getEventFieldsAsString(){
		return this.eventFieldsAsString;
	}
	
	
    @ManagedAttribute(description = "the string that represents a null value when eventFieldsAsString = true")
	public void setNullString(String nullString){
		this.nullString = nullString;
	}

    @ManagedAttribute(description = "the string that represents a null value when eventFieldsAsString = true")
	public String getNullString(){
		return this.nullString;
	}	

    @ManagedAttribute(description = "the date format to use when eventFieldsAsString = true, as required by java.text.SimpleDateFormat")
	public void setDateFormat(String dateFormat){
		this.dateFormat = dateFormat;
	}

    @ManagedAttribute(description = "the date format to use when eventFieldsAsString = true, as required by java.text.SimpleDateFormat")
	public String getDateFormat(){
		return this.dateFormat;
	}	

    @ManagedAttribute(description = "the timezone to set on SimpleDateFormat when eventFieldsAsString = true")
	public void setDateTimezone(String dateTimezone){
		this.dateTimezone = dateTimezone;
	}

    @ManagedAttribute(description = "the timezone set on SimpleDateFormat when eventFieldsAsString = true")
	public String getDateTimezone(){
		return this.dateTimezone;
	}	
    
	
    @ManagedAttribute(description = "if the endpoint should try to recover in case of error")
	public void setAutoRecover(boolean autoRecover){
		this.autoRecover = autoRecover;
	}

    @ManagedAttribute(description = "if the endpoint should try to recover in case of error")
	public boolean getAutoRecover(){
		return this.autoRecover;
	}
    

    @ManagedAttribute(description = "the interval for the auto recover thread, if autoRecover=true")
	public void setAutoInterval(long autoRecoverInterval){
		this.autoRecoverInterval = autoRecoverInterval;
	}

    @ManagedAttribute(description = "the interval for the auto recover thread, if autoRecover=true")
	public long getAutoRecoverInterval(){
		return this.autoRecoverInterval;
	}
    

    @ManagedAttribute(description = "if the endpoint should start if it cannot get a connection to dfESP")
	public void setForceStart(boolean forceStart){
		this.forceStart = forceStart;
	}

    @ManagedAttribute(description = "if the endpoint should start if it cannot get a connection to dfESP")
	public boolean getForceStart(){
		return this.forceStart;
	}
    
    
    
    
    
    
    

}
