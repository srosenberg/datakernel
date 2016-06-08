package io.datakernel.simplefs.stress;

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.RunnableWithException;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.simplefs.SimpleFsServer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestTimeoutsSimpleFs {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path storagePath;
	private byte[] BIG_FILE = new byte[256 * 1024 * 1024];
	private ExecutorService executorService = Executors.newCachedThreadPool();

	{
		Random rand = new Random(1L);
		for (int i = 0; i < BIG_FILE.length; i++) {
			BIG_FILE[i] = (byte) (rand.nextInt(256) - 128);
		}
	}

	@Before
	public void setUp() throws IOException {
		storagePath = Paths.get(temporaryFolder.newFolder("server_storage").toURI());
	}

	@Test
	public void testUploadTimeout() {
		InetSocketAddress address = new InetSocketAddress(7000);
		Eventloop eventloop = new Eventloop();
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		startAcceptOnceServer();

		client.upload("fileName.txt", StreamProducers.ofValue(eventloop, ByteBuf.wrap(BIG_FILE)), new CompletionCallback() {
			@Override
			public void onComplete() {
			}

			@Override
			public void onException(Exception e) {
			}
		});

		eventloop.run();
		executorService.shutdown();
	}

	private void startAcceptOnceServer() {
		final Eventloop serverEventloop = new Eventloop();
		final ExecutorService serverExecutor = Executors.newFixedThreadPool(2);
		final SimpleFsServer server = new SimpleFsServer(serverEventloop, serverExecutor, storagePath)
				.setReadTimeout(1)      // ensure timeout exception
				.setWriteTimeout(1)
				.acceptOnce()
				.setListenPort(7000);

		executorService.submit(new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				server.listen();
				serverEventloop.run();
				serverExecutor.shutdown();
			}
		});
	}
}
