/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import io.datakernel.async.CompletionCallback;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.guice.servicegraph.AsyncServiceAdapters;
import io.datakernel.guice.servicegraph.ServiceGraphModule;
import io.datakernel.guice.servicegraph.SingletonService;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraph;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static io.datakernel.util.Preconditions.checkState;

/**
 * Example of booting application which depends on several services.
 *
 * Application calls method of {@link io.datakernel.example.ServiceWithDependencies},
 * which depends on {@link io.datakernel.example.BasicServiceA} and {@link io.datakernel.example.BasicServiceB}.
 * {@link io.datakernel.example.LauncherExample.ServicesLauncher} extends {@link io.datakernel.launcher.Launcher}
 * which helps to load configurations, manage services and start them in proper order.
 *
 */
public class LauncherExample {

	public static void main(String[] args) throws Exception {
		disableExternalLoggers();
		Launcher.run(ServicesLauncher.class, args);
	}

	public static class ServicesLauncher extends Launcher {

		@Inject
		private ServiceWithDependencies rootService;

		@Override
		protected void configure() {
			configs("launcher-example-config.properties");
			modules(new ServiceGraphModule()
							.register(ServiceWithDependencies.class, AsyncServiceAdapters.forNioService())
							.register(BasicServiceA.class, AsyncServiceAdapters.forNioService())
							.register(BasicServiceB.class, AsyncServiceAdapters.forNioService())
							.register(NioEventloop.class, AsyncServiceAdapters.forNioEventloop()),
					new LauncherExampleModule());

		}

		@Override
		protected void doRun() throws Exception {
			rootService.makeComputation();
		}
	}

	private static void disableExternalLoggers() {
		Logger serviceGraphModuleLogger = (Logger) LoggerFactory.getLogger(ServiceGraphModule.class);
		serviceGraphModuleLogger.setLevel(Level.OFF);
		Logger serviceGraphLogger = (Logger) LoggerFactory.getLogger(ServiceGraph.class);
		serviceGraphLogger.setLevel(Level.OFF);
		Logger eventloopLogger = (Logger) LoggerFactory.getLogger(NioEventloop.class);
		eventloopLogger.setLevel(Level.OFF);
		Logger launcherLogger = (Logger) LoggerFactory.getLogger(ServicesLauncher.class);
		launcherLogger.setLevel(Level.OFF);
	}
}
