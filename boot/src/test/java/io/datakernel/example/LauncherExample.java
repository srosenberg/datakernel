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

public class LauncherExample {

	public static void main(String[] args) throws Exception {
		disableExternalLoggers();
		Launcher.run(ServicesLauncher.class, args);
	}

	private static void disableExternalLoggers() {
		Logger serviceGraphModuleLogger = (Logger) LoggerFactory.getLogger(ServiceGraphModule.class);
		serviceGraphModuleLogger.setLevel(Level.OFF);
		Logger serviceGraphLogger = (Logger) LoggerFactory.getLogger(ServiceGraph.class);
		serviceGraphLogger.setLevel(Level.OFF);
		Logger eventloopLogger = (Logger) LoggerFactory.getLogger(NioEventloop.class);
		eventloopLogger.setLevel(Level.OFF);
	}

	public static class ServicesLauncher extends Launcher {

		@Inject
		private ServiceWithDependencies rootService;
		@Inject
		private NioEventloop eventloop;

		@Override
		protected void configure() {
			configs("launcher-example-config.properties");
			modules(new ServiceGraphModule()
							.register(ServiceWithDependencies.class, AsyncServiceAdapters.forNioService())
							.register(BasicServiceA.class, AsyncServiceAdapters.forNioService())
							.register(BasicServiceB.class, AsyncServiceAdapters.forNioService())
							.register(NioEventloop.class, AsyncServiceAdapters.forNioEventloop()),
					new EventloopModule(),
					new ServicesModule());

		}

		@Override
		protected void doStart() throws Exception {
			super.doStart();
		}

		@Override
		protected void doRun() throws Exception {
			rootService.makeComputation();
		}
	}

	public static class ServiceWithDependencies implements NioService {
		private final NioEventloop eventloop;
		private final BasicServiceA serviceA;
		private final BasicServiceB serviceB;
		private boolean started;

		public ServiceWithDependencies(NioEventloop eventloop, BasicServiceA serviceA, BasicServiceB serviceB) {
			this.eventloop = eventloop;
			this.serviceA = serviceA;
			this.serviceB = serviceB;
		}

		@Override
		public NioEventloop getNioEventloop() {
			return eventloop;
		}

		@Override
		public void start(final CompletionCallback callback) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					started = true;
					System.out.println("ServiceWithDependencies started");
					callback.onComplete();
				}
			});
		}

		@Override
		public void stop(final CompletionCallback callback) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					callback.onComplete();
				}
			});
		}

		public void makeComputation() {
			checkState(started);
			System.out.println("ServiceWithDependencies.makeComputation() invoked");
			serviceA.makeComputation();
			serviceB.makeComputation();
		}
	}

	public static class BasicServiceA implements NioService {
		private final NioEventloop eventloop;
		private final int startDelayMsec;
		private boolean started;

		public BasicServiceA(int startDelayMsec, NioEventloop eventloop) {
			this.eventloop = eventloop;
			this.startDelayMsec = startDelayMsec;
			this.started = false;
		}

		@Override
		public NioEventloop getNioEventloop() {
			return eventloop;
		}

		@Override
		public void start(final CompletionCallback callback) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					eventloop.schedule(new Date().getTime() + startDelayMsec, new Runnable() {
						@Override
						public void run() {
							started = true;
							System.out.println("BasicServiceA started");
							callback.onComplete();
						}
					});
				}
			});
		}

		@Override
		public void stop(final CompletionCallback callback) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					callback.onComplete();
				}
			});
		}

		public void makeComputation() {
			checkState(started);
			System.out.println("BasicServiceA.makeComputation() invoked");
		}
	}

	public static class BasicServiceB implements NioService {
		private final NioEventloop eventloop;
		private boolean started;

		public BasicServiceB(NioEventloop eventloop) {
			this.eventloop = eventloop;
			this.started = false;
		}

		@Override
		public NioEventloop getNioEventloop() {
			return eventloop;
		}

		@Override
		public void start(final CompletionCallback callback) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					started = true;
					System.out.println("BasicServiceB started");
					callback.onComplete();
				}
			});
		}

		@Override
		public void stop(final CompletionCallback callback) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					callback.onComplete();
				}
			});
		}

		public void makeComputation() {
			checkState(started);
			System.out.println("BasicServiceB.makeComputation() invoked");
		}
	}

	public static class EventloopModule extends AbstractModule {

		@Override
		protected void configure() {

		}

		@Provides
		@SingletonService
		public NioEventloop nioEventloop() {
			return new NioEventloop();
		}
	}

	public static class ServicesModule extends AbstractModule {

		@Override
		protected void configure() {

		}

		@Provides
		@SingletonService
		public BasicServiceA basicServiceA(NioEventloop eventloop, Config config) {
			Config delayConfig = config.getChild("basicServiceA.delay");
			int delay = ConfigConverters.ofInteger().get(delayConfig);
			return new BasicServiceA(delay, eventloop);
		}

		@Provides
		@SingletonService
		public BasicServiceB basicServiceB(NioEventloop eventloop) {
			return new BasicServiceB(eventloop);
		}

		@Provides
		@SingletonService
		public ServiceWithDependencies serviceWithDependencies(BasicServiceA serviceA, BasicServiceB serviceB,
		                                                       NioEventloop eventloop) {
			return new ServiceWithDependencies(eventloop, serviceA, serviceB);
		}
	}

}
