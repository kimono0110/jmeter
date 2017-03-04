/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jmeter.visualizers.backend.elasticsearch;

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.SamplerMetric;
import org.apache.jmeter.visualizers.backend.UserMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link AbstractBackendListenerClient} to write in an Elasticsearch using 
 * custom schema
 * @since 3.2
 */
public class ElasticsearchBackendListenerClient extends AbstractBackendListenerClient implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchBackendListenerClient.class);
    private ConcurrentHashMap<String, SamplerMetric> metricsPerSampler = new ConcurrentHashMap<>();
    // Name of the measurement
    private static final String EVENTS_FOR_ANNOTATION = "events";
    
    private static final long SEND_INTERVAL = JMeterUtils.getPropDefault("backend_influxdb.send_interval", 5);
    private static final int MAX_POOL_SIZE = 1;
    private static final Object LOCK = new Object();

    private static final String DEFAULT_ELASTICSEARCH_HOST = "localhost";
    private static final int DEFAULT_ELASTICSEARCH_PORT = 9200;
    
    private boolean summaryOnly;
    private String measurement = "DEFAULT_MEASUREMENT";
    private String elasticsearchHost;
    private int  elasticsearchPort;
    private String samplersRegex = "";
    private Pattern samplersToFilter;
    private Map<String, Float> allPercentiles;
    // Name of the application tested

    private ElasticsearchMetricsSender elasticsearchMetricsManager;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timerHandle;

    public ElasticsearchBackendListenerClient() {
        super();
    }

    @Override
    public void run() {
        sendMetrics();
    }

    /**
     * Send metrics
     */
    protected void sendMetrics() {

        synchronized (LOCK) {
            for (Map.Entry<String, SamplerMetric> entry : getMetricsInfluxdbPerSampler().entrySet()) {
                SamplerMetric metric = entry.getValue();
                // We are computing on interval basis so cleanup
                metric.resetForTimeInterval();
            }
        }

        // For JMETER context
        StringBuilder tag = new StringBuilder(60);
        StringBuilder field = new StringBuilder(80);

        elasticsearchMetricsManager.addMetric(measurement, tag.toString(), field.toString());

        elasticsearchMetricsManager.writeAndSendMetrics();
    }

    /**
     * Add request metrics to metrics manager.
     * 
     * @param metric
     *            {@link SamplerMetric}
     */
    private void addMetrics(String transaction, SamplerMetric metric) {
    }

    private void addMetric(String transaction, SamplerMetric metric, int count, boolean includeResponseCode,
            String statut, double mean, double minTime, double maxTime, Collection<Float> pcts) {
        if (count > 0) {
            StringBuilder tag = new StringBuilder(70);
            StringBuilder field = new StringBuilder(80);
            elasticsearchMetricsManager.addMetric(measurement, tag.toString(), field.toString());
        }
    }

    private void addCumulatedMetrics(SamplerMetric metric) {
        int total = metric.getTotal();
        if (total > 0) {
            StringBuilder tag = new StringBuilder(70);
            StringBuilder field = new StringBuilder(100);
            Collection<Float> pcts = allPercentiles.values();
            elasticsearchMetricsManager.addMetric(measurement, tag.toString(), field.toString());
        }
    }

    /**
     * @return the samplersList
     */
    public String getSamplersRegex() {
        return samplersRegex;
    }

    /**
     * @param samplersList
     *            the samplersList to set
     */
    public void setSamplersList(String samplersList) {
        this.samplersRegex = samplersList;
    }

    @Override
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        synchronized (LOCK) {
            UserMetric userMetrics = getUserMetrics();
            for (SampleResult sampleResult : sampleResults) {
                userMetrics.add(sampleResult);
                Matcher matcher = samplersToFilter.matcher(sampleResult.getSampleLabel());
                if (!summaryOnly && (matcher.find())) {
                    SamplerMetric samplerMetric = getSamplerMetricInfluxdb(sampleResult.getSampleLabel());
                    samplerMetric.add(sampleResult);
                }
           }
        }
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        String elasticsearchMetricsSender = context.getParameter("elasticsearchMetricsSender");
        elasticsearchHost = context.getParameter("elasticsearchHost");
        elasticsearchPort = context.getIntParameter("elasticsearchPort", DEFAULT_ELASTICSEARCH_PORT);
        log.info(context.getParameter("elasticsearchIndex"));
        Class<?> clazz = Class.forName(elasticsearchMetricsSender);
        this.elasticsearchMetricsManager = (ElasticsearchMetricsSender) clazz.newInstance();
        elasticsearchMetricsManager.setup(elasticsearchHost, elasticsearchPort);

        scheduler = Executors.newScheduledThreadPool(MAX_POOL_SIZE);
        // Start immediately the scheduler and put the pooling ( 5 seconds by default )
        this.timerHandle = scheduler.scheduleAtFixedRate(this, 0, SEND_INTERVAL, TimeUnit.SECONDS);

    }

    protected SamplerMetric getSamplerMetricInfluxdb(String sampleLabel) {
        SamplerMetric samplerMetric = metricsPerSampler.get(sampleLabel);
        if (samplerMetric == null) {
            samplerMetric = new SamplerMetric();
            SamplerMetric oldValue = metricsPerSampler.putIfAbsent(sampleLabel, samplerMetric);
            if (oldValue != null) {
                samplerMetric = oldValue;
            }
        }
        return samplerMetric;
    }

    private Map<String, SamplerMetric> getMetricsInfluxdbPerSampler() {
        return metricsPerSampler;
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        boolean cancelState = timerHandle.cancel(false);
        log.debug("Canceled state: {}", cancelState);
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Error waiting for end of scheduler");
            Thread.currentThread().interrupt();
        }

        addAnnotation(false);

        // Send last set of data before ending
        log.info("Sending last metrics");
        sendMetrics();

        elasticsearchMetricsManager.destroy();
        super.teardownTest(context);
    }

    /**
     * Add Annotation at start or end of the run ( usefull with Grafana )
     * Grafana will let you send HTML in the text such as a link to the release notes
     * Tags are separated by spaces in grafana
     * Tags is put as InfluxdbTag for better query performance on it
     * Never double or single quotes in influxdb except for string field
     * see : https://docs.influxdata.com/influxdb/v1.1/write_protocols/line_protocol_reference/#quoting-special-characters-and-additional-naming-guidelines
     * * @param startOrEnd boolean true for start, false for end
     */
    private void addAnnotation(boolean startOrEnd) {
    }
    
    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        arguments.addArgument("elasticsearchMetricsSender", HttpMetricsSender.class.getName());
        arguments.addArgument("elasticsearchHost", DEFAULT_ELASTICSEARCH_HOST);
        arguments.addArgument("elasticsearchPort", Integer.toString(DEFAULT_ELASTICSEARCH_PORT));
        arguments.addArgument("elasticsearchIndex", "jmeter-${__time(YMDHMS)}");
        return arguments;
    }
}
