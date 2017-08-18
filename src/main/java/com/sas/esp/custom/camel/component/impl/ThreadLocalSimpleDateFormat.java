package com.sas.esp.custom.camel.component.impl;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class ThreadLocalSimpleDateFormat {
	
	private String dateFormat;
	private String dateTimezone;
	
	public ThreadLocalSimpleDateFormat(String dateFormat, String dateTimezone) {
		this.dateFormat = dateFormat;
		this.dateTimezone = dateTimezone;
	}

	private ThreadLocal<SimpleDateFormat> dateFormatHolder = new ThreadLocal<SimpleDateFormat>() {

		@Override
		protected SimpleDateFormat initialValue() {
			
			SimpleDateFormat dateFormatter = new SimpleDateFormat(dateFormat);
			
			if (dateTimezone != null) {
				dateFormatter.setTimeZone(TimeZone.getTimeZone(dateTimezone));	 
			}
			
			return dateFormatter;
		}
	};


	public SimpleDateFormat getDateFormatter() {
		return dateFormatHolder.get();
	}

}
