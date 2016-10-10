package io.datakernel.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;

import javax.inject.Singleton;

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

class MyAbstractModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    Eventloop eventloop() {
        return Eventloop.create();
    }

    @Provides
    @Singleton
    AsyncHttpServer httpServer(Eventloop eventloop, AsyncServlet servlet) {
        return AsyncHttpServer.create(eventloop, servlet)
                .withListenPort(HttpHelloWorldLauncher.PORT);
    }

    @Provides
    @Singleton
    AsyncServlet httpServlet() {
        return new AsyncServlet() {
            @Override
            public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
                callback.setResult(HttpResponse.ok200().withBody(encodeAscii("Hello, World!")));
            }
        };
    }
}
