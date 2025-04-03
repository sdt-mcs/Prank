package org.apache.hadoop.yarn.prank.anomaly;

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.prank.config.PrankConfiguration;
import org.apache.hadoop.yarn.prank.monitoring.MetricsCollector;
import org.apache.hadoop.yarn.prank.monitoring.PerformanceMetrics;
import org.apache.hadoop.yarn.prank.topology.DLRATopology;
import org.apache.hadoop.yarn.prank.topology.MicroserviceComponent;

/**
 * Identifies the root causes of performance anomalies in DLRA microservices
 * using a customized PageRank algorithm that considers correlation coefficients,
 * service dependencies, and anomaly scores.
 */
public class RootCauseLocator {
    private static final Log LOG = LogFactory.getLog(RootCauseLocator.class);
    private final MetricsCollector metricsCollector;
    private final PrankConfiguration config;
    
    // Default parameters for PageRank algorithm
    private static final double DAMPING_FACTOR = 0.85;
    private static final double CONVERGENCE_THRESHOLD = 1e-3;
    private static final int MAX_ITERATIONS = 100;
    
    // Weights for correlation and call frequency in edge weight calculation
    private static final double ALPHA = 0.5;
    private static final double BETA = 0.5;
    
    // Small constant to avoid division by zero in calculations
    private static final double EPSILON = 0.001;
    
    public RootCauseLocator(MetricsCollector metricsCollector, PrankConfiguration config) {
        this.metricsCollector = metricsCollector;
        this.config = config;
    }
    
    /**
     * Identify root causes of anomalies in a DLRA topology
     * @param topology DLRA topology with components (microservices)
     * @param anomalousComponents List of components with detected anomalies
     * @return List of components ranked by likelihood of being root causes
     */
    public List<MicroserviceAnomalySource> locateRootCauses(
            DLRATopology topology, 
            List<MicroserviceComponent> anomalousComponents) {
        
        LOG.info("Locating root causes for " + anomalousComponents.size() + 
                 " anomalous components in DLRA: " + topology.getName());
        
        if (anomalousComponents.isEmpty()) {
            LOG.info("No anomalous components provided, returning empty list");
            return Collections.emptyList();
        }
        
        // Get correlation coefficients between microservices and DLRA performance
        Map<String, Double> correlationCoefficients = calculateCorrelationCoefficients(
                topology, anomalousComponents);
        
        // Build service call graph for PageRank calculation
        Map<String, Map<String, Double>> callGraph = buildServiceCallGraph(
                topology, anomalousComponents, correlationCoefficients);
        
        // Calculate PageRank scores to identify root causes
        Map<String, Double> importanceScores = calculatePageRank(
                callGraph, anomalousComponents);
        
        // Create and rank anomaly sources
        List<MicroserviceAnomalySource> rootCauses = rankRootCauses(
                topology, anomalousComponents, importanceScores);
        
        LOG.info("Identified " + rootCauses.size() + " potential root causes");
        for (int i = 0; i < Math.min(3, rootCauses.size()); i++) {
            MicroserviceAnomalySource cause = rootCauses.get(i);
            LOG.info("Root cause #" + (i+1) + ": " + cause.getComponentId() + 
                     ", score: " + cause.getImportanceScore());
        }
        
        return rootCauses;
    }
    
    /**
     * Calculate Pearson correlation coefficients between microservice components
     * and overall DLRA performance
     */
    private Map<String, Double> calculateCorrelationCoefficients(
            DLRATopology topology, 
            List<MicroserviceComponent> anomalousComponents) {
        
        LOG.info("Calculating correlation coefficients for " + 
                 anomalousComponents.size() + " components");
        
        Map<String, Double> correlationCoefficients = new HashMap<>();
        Map<String, List<Double>> componentLatencies = new HashMap<>();
        List<Double> dlraLatencies = new ArrayList<>();
        
        // Get historical performance metrics for correlation calculation
        int windowSize = config.getCorrelationWindowSize();
        Map<String, List<PerformanceMetrics>> metricsHistory = 
                metricsCollector.getPerformanceHistory(topology, windowSize);
        
        // Extract latency time series for components and overall DLRA
        for (MicroserviceComponent component : anomalousComponents) {
            String componentId = component.getId();
            List<PerformanceMetrics> metrics = metricsHistory.get(componentId);
            
            if (metrics == null || metrics.isEmpty()) {
                LOG.warn("No performance history for component: " + componentId);
                correlationCoefficients.put(componentId, 0.0);
                continue;
            }
            
            List<Double> latencies = new ArrayList<>();
            for (PerformanceMetrics m : metrics) {
                latencies.add(m.getTailLatency());
                
                // Add to DLRA latencies if this is the entry point component
                if (component.isEntryPoint() && dlraLatencies.size() < metrics.size()) {
                    dlraLatencies.add(m.getEndToEndLatency());
                }
            }
            
            componentLatencies.put(componentId, latencies);
        }
        
        // If DLRA latencies not populated from entry point, get from metrics collector
        if (dlraLatencies.isEmpty()) {
            dlraLatencies = metricsCollector.getDLRALatencyHistory(topology, windowSize);
        }
        
        // Calculate Pearson correlation coefficients
        for (MicroserviceComponent component : anomalousComponents) {
            String componentId = component.getId();
            List<Double> componentLatency = componentLatencies.get(componentId);
            
            if (componentLatency == null || dlraLatencies.isEmpty() || 
                componentLatency.size() != dlraLatencies.size()) {
                LOG.warn("Insufficient data for correlation: " + componentId);
                correlationCoefficients.put(componentId, 0.0);
                continue;
            }
            
            double correlation = calculatePearsonCorrelation(
                    componentLatency, dlraLatencies);
            
            LOG.info("Correlation coefficient for " + componentId + ": " + correlation);
            correlationCoefficients.put(componentId, correlation);
        }
        
        return correlationCoefficients;
    }
    
    /**
     * Calculate Pearson correlation coefficient between two time series
     */
    private double calculatePearsonCorrelation(List<Double> series1, List<Double> series2) {
        if (series1.size() != series2.size() || series1.isEmpty()) {
            return 0.0;
        }
        
        int n = series1.size();
        
        // Calculate means
        double mean1 = 0.0, mean2 = 0.0;
        for (int i = 0; i < n; i++) {
            mean1 += series1.get(i);
            mean2 += series2.get(i);
        }
        mean1 /= n;
        mean2 /= n;
        
        // Calculate Pearson correlation coefficient
        double numerator = 0.0;
        double denom1 = 0.0;
        double denom2 = 0.0;
        
        for (int i = 0; i < n; i++) {
            double diff1 = series1.get(i) - mean1;
            double diff2 = series2.get(i) - mean2;
            
            numerator += diff1 * diff2;
            denom1 += diff1 * diff1;
            denom2 += diff2 * diff2;
        }
        
        // Handle zero denominator case
        if (denom1 == 0 || denom2 == 0) {
            return 0.0;
        }
        
        return numerator / Math.sqrt(denom1 * denom2);
    }
    
    /**
     * Build service call graph with weighted edges for PageRank calculation
     */
    private Map<String, Map<String, Double>> buildServiceCallGraph(
            DLRATopology topology,
            List<MicroserviceComponent> anomalousComponents,
            Map<String, Double> correlationCoefficients) {
        
        LOG.info("Building service call graph for " + 
                 anomalousComponents.size() + " components");
        
        Map<String, Map<String, Double>> callGraph = new HashMap<>();
        Set<String> anomalousIds = new HashSet<>();
        
        // Initialize the call graph with empty adjacency lists
        for (MicroserviceComponent component : anomalousComponents) {
            String componentId = component.getId();
            anomalousIds.add(componentId);
            callGraph.put(componentId, new HashMap<>());
        }
        
        // Add edges based on service call relationships
        for (MicroserviceComponent source : anomalousComponents) {
            String sourceId = source.getId();
            
            // Get outgoing calls (downstream services)
            for (String targetId : source.getDownstreamComponents()) {
                if (!anomalousIds.contains(targetId)) {
                    continue; // Skip non-anomalous targets
                }
                
                // Calculate call frequency factor
                double callFrequency = source.getCallFrequency(targetId);
                
                // Get latency ratio between services
                double sourceLatency = metricsCollector.getLatestMetrics(sourceId).getTailLatency();
                double targetLatency = metricsCollector.getLatestMetrics(targetId).getTailLatency();
                
                // Calculate call influence using Eq. 3 from the paper
                double latencyRatio = Math.log(targetLatency + EPSILON) / 
                                      Math.log(sourceLatency + EPSILON);
                double callInfluence = callFrequency * latencyRatio;
                
                // Get correlation coefficients
                double sourceCorrelation = correlationCoefficients.getOrDefault(sourceId, 0.0);
                double targetCorrelation = correlationCoefficients.getOrDefault(targetId, 0.0);
                
                // Calculate edge weight using Eq. 4 from the paper
                Map<String, Double> outgoingCalls = callGraph.get(sourceId);
                outgoingCalls.put(targetId, callInfluence);
                
                // Store for normalization
                callGraph.put(sourceId, outgoingCalls);
            }
        }
        
        // Add self-loops as described in Eq. 5
        for (MicroserviceComponent component : anomalousComponents) {
            String componentId = component.getId();
            Map<String, Double> outgoingCalls = callGraph.get(componentId);
            
            // Calculate standardized anomaly score
            double anomalyScore = calculateAnomalyScore(component);
            double normalizedScore = 0.0;
            
            // Find max anomaly score for normalization
            double maxScore = 0.0;
            for (MicroserviceComponent c : anomalousComponents) {
                double score = calculateAnomalyScore(c);
                maxScore = Math.max(maxScore, score);
            }
            
            // Normalize anomaly score
            if (maxScore > 0) {
                normalizedScore = anomalyScore / maxScore;
            }
            
            // Calculate self-loop weight
            double selfCorrelation = correlationCoefficients.getOrDefault(componentId, 0.0);
            
            // Calculate max correlation with downstream components
            double maxDownstreamCorrelation = 0.0;
            for (String targetId : component.getDownstreamComponents()) {
                if (anomalousIds.contains(targetId)) {
                    double targetCorrelation = correlationCoefficients.getOrDefault(targetId, 0.0);
                    maxDownstreamCorrelation = Math.max(maxDownstreamCorrelation, targetCorrelation);
                }
            }
            
            // Self-loop correlation factor
            double correlationFactor = Math.max(0.0, selfCorrelation - maxDownstreamCorrelation);
            
            // Calculate self-loop weight using normalized anomaly score
            double selfWeight = normalizedScore / (normalizedScore + 
                             outgoingCalls.values().stream().mapToDouble(d -> d).sum());
            
            // Add self-loop
            outgoingCalls.put(componentId, selfWeight);
            callGraph.put(componentId, outgoingCalls);
        }
        
        // Normalize outgoing edge weights to sum to 1 for each node
        for (String sourceId : callGraph.keySet()) {
            Map<String, Double> outgoingCalls = callGraph.get(sourceId);
            double sum = outgoingCalls.values().stream().mapToDouble(d -> d).sum();
            
            if (sum > 0) {
                Map<String, Double> normalizedCalls = new HashMap<>();
                for (Map.Entry<String, Double> entry : outgoingCalls.entrySet()) {
                    normalizedCalls.put(entry.getKey(), entry.getValue() / sum);
                }
                callGraph.put(sourceId, normalizedCalls);
            }
        }
        
        return callGraph;
    }
    
    /**
     * Calculate anomaly score for a component based on tail latency standard deviation ratio
     */
    private double calculateAnomalyScore(MicroserviceComponent component) {
        PerformanceMetrics currentMetrics = metricsCollector.getLatestMetrics(component.getId());
        PerformanceMetrics previousMetrics = metricsCollector.getPreviousMetrics(component.getId());
        
        if (currentMetrics == null || previousMetrics == null) {
            return 0.0;
        }
        
        double currentStdDev = currentMetrics.getTailLatencyStdDev();
        double previousStdDev = previousMetrics.getTailLatencyStdDev();
        
        if (previousStdDev <= 0) {
            return 0.0;
        }
        
        // Calculate theta (standard deviation ratio)
        double theta = currentStdDev / previousStdDev;
        
        // Calculate anomaly score
        return Math.max(0.0, theta - config.getAnomalyThreshold());
    }
    
    /**
     * Calculate PageRank scores for components to identify root causes
     */
    private Map<String, Double> calculatePageRank(
            Map<String, Map<String, Double>> callGraph,
            List<MicroserviceComponent> anomalousComponents) {
        
        LOG.info("Calculating PageRank scores for " + callGraph.size() + " components");
        
        Map<String, Double> scores = new HashMap<>();
        Map<String, Double> nextScores = new HashMap<>();
        
        // Initialize PageRank scores based on anomaly detection
        // Components with anomalies get initial score of 1.0
        for (MicroserviceComponent component : anomalousComponents) {
            String componentId = component.getId();
            scores.put(componentId, 1.0);
        }
        
        // Normalize initial scores
        double sumScores = scores.values().stream().mapToDouble(d -> d).sum();
        if (sumScores > 0) {
            for (String componentId : scores.keySet()) {
                scores.put(componentId, scores.get(componentId) / sumScores);
            }
        }
        
        // Iterative PageRank calculation
        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            // Calculate next scores
            for (String componentId : callGraph.keySet()) {
                double score = 1.0 - DAMPING_FACTOR; // Random jump probability
                
                // Add scores from incoming edges
                for (String sourceId : callGraph.keySet()) {
                    if (callGraph.get(sourceId).containsKey(componentId)) {
                        double weight = callGraph.get(sourceId).get(componentId);
                        score += DAMPING_FACTOR * scores.get(sourceId) * weight;
                    }
                }
                
                nextScores.put(componentId, score);
            }
            
            // Check for convergence
            double diff = 0.0;
            for (String componentId : scores.keySet()) {
                diff = Math.max(diff, Math.abs(scores.get(componentId) - 
                                              nextScores.get(componentId)));
            }
            
            // Update scores for next iteration
            scores = new HashMap<>(nextScores);
            
            LOG.info("PageRank iteration " + iteration + 
                     ", max diff: " + diff);
            
            if (diff < CONVERGENCE_THRESHOLD) {
                LOG.info("PageRank converged after " + iteration + " iterations");
                break;
            }
        }
        
        return scores;
    }
    
    /**
     * Rank components by their importance scores to identify root causes
     */
    private List<MicroserviceAnomalySource> rankRootCauses(
            DLRATopology topology,
            List<MicroserviceComponent> anomalousComponents,
            Map<String, Double> importanceScores) {
        
        List<MicroserviceAnomalySource> rootCauses = new ArrayList<>();
        
        // Create anomaly source objects with scores
        for (MicroserviceComponent component : anomalousComponents) {
            String componentId = component.getId();
            double score = importanceScores.getOrDefault(componentId, 0.0);
            
            MicroserviceAnomalySource anomalySource = 
                    new MicroserviceAnomalySource(componentId, score);
            
            // Add containers for this component
            anomalySource.setContainerIds(component.getContainerIds());
            
            rootCauses.add(anomalySource);
        }
        
        // Sort by importance score (descending)
        Collections.sort(rootCauses, (a, b) -> 
                Double.compare(b.getImportanceScore(), a.getImportanceScore()));
        
        // Select top K components as root causes based on Prank configuration
        int k = Math.min(rootCauses.size(), 
                        Math.max(1, (int)Math.ceil(anomalousComponents.size() * 0.2)));
        
        LOG.info("Selected top " + k + " components as root causes");
        
        return rootCauses.subList(0, k);
    }
}
