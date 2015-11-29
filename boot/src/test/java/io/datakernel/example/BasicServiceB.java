package io.datakernel.example;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;

import static io.datakernel.util.Preconditions.checkState;

/**
 * This service starts immediately.
 */
public class BasicServiceB implements NioService {
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
