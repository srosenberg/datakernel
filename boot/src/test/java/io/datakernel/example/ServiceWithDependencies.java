package io.datakernel.example;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;

import static io.datakernel.util.Preconditions.checkState;

/**
 * This service should start after {@link io.datakernel.example.BasicServiceA} and
 * {@link io.datakernel.example.BasicServiceB} started.
 */
public class ServiceWithDependencies implements NioService {
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