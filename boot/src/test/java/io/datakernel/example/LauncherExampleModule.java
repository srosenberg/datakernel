package io.datakernel.example;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.guice.servicegraph.SingletonService;

/**
 * Google Guice module that provides classes needed for {@link io.datakernel.example.LauncherExample}
 */
public class LauncherExampleModule extends AbstractModule {

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

	@Provides
	@SingletonService
	public NioEventloop nioEventloop() {
		return new NioEventloop();
	}
}
