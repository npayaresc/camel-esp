package com.sas.esp.custom.camel.component.impl;

import com.sas.esp.custom.camel.component.impl.dfESPEndpoint;

public interface dfESPAutoRecoverable {
	
	// the method that the auto recover agent will call
	void recover() throws Exception;
	
	// must return the dfESPEndpoint associated with the component
	dfESPEndpoint getEndpoint();

}
