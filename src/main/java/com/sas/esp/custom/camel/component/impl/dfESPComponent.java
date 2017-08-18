package com.sas.esp.custom.camel.component.impl;

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Represents the component that manages {@link dfESPEndpoint}.
 */

public class dfESPComponent extends UriEndpointComponent {

	public dfESPComponent() {
		super(dfESPEndpoint.class);
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		Endpoint endpoint = new dfESPEndpoint(uri, this);
		setProperties(endpoint, parameters);

		// bridge the dfESP PubSub logger (JUL) to SLF4J
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
		
		return endpoint;
	}
	
}
