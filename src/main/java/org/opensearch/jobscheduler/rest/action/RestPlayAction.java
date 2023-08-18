/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to trigger REST on another plugin
 */
public class RestPlayAction extends BaseRestHandler {
    private final Logger logger = LogManager.getLogger(RestPlayAction.class);

    public static final String PLAY_ACTION = "play";

    private final RestController restController;

    public RestPlayAction(RestController restController) {
        this.restController = restController;
    }

    @Override
    public String getName() {
        return PLAY_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        request.params().keySet().stream().forEach(request::param); // consume params
        HttpRequest httpRequest = new LocalHttpRequest(request.getHttpRequest(), Method.GET, "_cluster/health");
        RestRequest newRequest = new LocalRestRequest(request, httpRequest);
        LocalRestChannel restChannel = new LocalRestChannel(newRequest, true);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        this.restController.dispatchRequest(newRequest, restChannel, threadContext);

        return channel -> {
            channel.sendResponse(restChannel.restResponse());
            return;
        };
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            new Route(RestRequest.Method.GET, String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_foo"))
        );
    }

    private static class LocalHttpRequest implements HttpRequest {
        private final HttpRequest delegate;
        private final Method method;
        private final String uri;

        public LocalHttpRequest(HttpRequest request, Method method, String uri) {
            this.delegate = request;
            this.method = method;
            this.uri = uri;
        }

        @Override
        public Method method() {
            return this.method;
        }

        @Override
        public String uri() {
            return this.uri;
        }

        @Override
        public BytesReference content() {
            return delegate.content();
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return delegate.getHeaders();
        }

        @Override
        public List<String> strictCookies() {
            return delegate.strictCookies();
        }

        @Override
        public HttpVersion protocolVersion() {
            return delegate.protocolVersion();
        }

        @Override
        public HttpRequest removeHeader(String header) {
            return delegate.removeHeader(header);
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            return delegate.createResponse(status, content);
        }

        @Override
        public Exception getInboundException() {
            return delegate.getInboundException();
        }

        @Override
        public void release() {
            delegate.release();
        }

        @Override
        public HttpRequest releaseAndCopy() {
            return delegate.releaseAndCopy();
        }
    }

    private static class LocalRestRequest extends RestRequest {
        public LocalRestRequest(RestRequest request, HttpRequest httpRequest) {
            super(
                request.getXContentRegistry(),
                request.params(),
                httpRequest.uri(),
                request.getHeaders(),
                httpRequest,
                request.getHttpChannel()
            );
        }
    }

    private static class LocalRestChannel extends AbstractRestChannel {
        private final Logger logger = LogManager.getLogger(LocalRestChannel.class);

        private CountDownLatch latch = new CountDownLatch(1);
        private RestResponse restResponse;

        protected LocalRestChannel(RestRequest request, boolean detailedErrorsEnabled) {
            super(request, detailedErrorsEnabled);
            logger.error("Request: " + request.toString());
        }

        public RestResponse restResponse() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                return null;
            }
            return restResponse;
        }

        @Override
        public void sendResponse(RestResponse response) {
            logger.error("Response: " + response.toString());
            this.restResponse = response;
            latch.countDown();
        }
    }

}
