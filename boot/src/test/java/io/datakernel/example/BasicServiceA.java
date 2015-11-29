package io.datakernel.example;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;

import java.util.Date;

import static io.datakernel.util.Preconditions.checkState;

/**
 * This service will start after specified delay.
 */
public class BasicServiceA implements NioService {
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
