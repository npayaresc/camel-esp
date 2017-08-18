package com.sas.esp.custom.camel.component.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class dfESPAutoRecoverAgent {

	// Logger
	private static final Logger logger = LoggerFactory.getLogger(dfESPAutoRecoverAgent.class);

	// Auto Recover
	private ScheduledExecutorService endpointAutoRecoverService;
	private ReentrantLock autoRecoverLock = new ReentrantLock();

	// The component to recover, that implements dfESPAutoRecoverable
	private dfESPAutoRecoverable component;

	// The interval to try to recover
	private long autoRecoverInterval;


	// Constructor
	public dfESPAutoRecoverAgent(dfESPAutoRecoverable component, long autoRecoverInterval) {
		this.component = component;
		this.autoRecoverInterval = autoRecoverInterval;
	}


	// Method to start an endpointAutoRecoverTask in background
	public synchronized void start() {

		if (endpointAutoRecoverService == null) {
			logger.info("starting auto recover thread for endpoint [{}]", component.getEndpoint().getEndpointUri());
			endpointAutoRecoverService = component.getEndpoint().getCamelContext().getExecutorServiceManager().newScheduledThreadPool(this, component.getEndpoint().getEndpointUri() + "_agent", 1);
			Runnable endpointAutoRecoverTask = new endpointAutoRecoverThread();
			endpointAutoRecoverService.scheduleAtFixedRate(endpointAutoRecoverTask, autoRecoverInterval, autoRecoverInterval, TimeUnit.MILLISECONDS);
			logger.info("started auto recover thread for endpoint [{}]", component.getEndpoint().getEndpointUri());
		}

	}


	// Method to stop the endpointAutoRecoverTask
	public synchronized void stop() {

		if (endpointAutoRecoverService != null) {
			logger.info("stopping auto recover thread for endpoint [{}]", component.getEndpoint().getEndpointUri());
			component.getEndpoint().getCamelContext().getExecutorServiceManager().shutdown(endpointAutoRecoverService);
			endpointAutoRecoverService = null;
			logger.info("stopped auto recover thread for endpoint [{}]", component.getEndpoint().getEndpointUri());
		}

	}


	// Auto Recover Thread
	private final class endpointAutoRecoverThread implements Runnable {


		public endpointAutoRecoverThread() {
		}

		public void run() {

			// only run if CamelContext has been fully started
			if (!component.getEndpoint().getCamelContext().getStatus().isStarted()) {
				if (logger.isDebugEnabled()) {
					logger.debug("auto recover thread cannot run for endpoint [{}] because CamelContext({}) has not been started yet", component.getEndpoint().getEndpointUri(), component.getEndpoint().getCamelContext().getName());
				}
				return;
			}

			// get a lock
			autoRecoverLock.lock();

			try {
				// try to recover
				logger.info("trying to recover endpoint [{}] ", component.getEndpoint().getEndpointUri());
				component.recover();
				logger.info("endpoint [{}] recovered successfully", component.getEndpoint().getEndpointUri());
				stop();
			} catch (Exception e) {
				// just log error if failed
				logger.error("error recovering endpoint [{}] [{}] ", component.getEndpoint().getEndpointUri(), e.getMessage());
			} finally {
				// release the lock
				autoRecoverLock.unlock();
			}

		}
	}


}
