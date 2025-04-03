package org.apache.hadoop.yarn.prank.anomaly;

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.prank.config.PrankConfiguration;
import org.apache.hadoop.yarn.prank.monitoring.PerformanceMetrics;
import org.apache.hadoop.yarn.prank.monitoring.MetricsCollector;
import org.apache.hadoop.yarn.prank.topology.DLRATopology;
import org.apache.hadoop.yarn.prank.topology.MicroserviceComponent;

/**
 * Anomaly detection for DLRA microservices based on statistical analysis
 * of performance metrics, particularly tail latency distributions
 */
public class AnomalyDetector {
    private static final Log LOG = LogFactory.getLog(AnomalyDetector.class);
    private final MetricsCollector metricsCollector;
    private final PrankConfiguration config;
    private final Map<String, PerformanceHistory> performanceHistories;
    
    public AnomalyDetector(MetricsCollector metricsCollector, PrankConfiguration config) {
        this.metricsCollector = metricsCollector;
        this.config = config;
        this.performanceHistories = new HashMap<>();
    }
    
    /**
     * Initialize performance histories for DLRA topology
     */
    public void initializeForTopology(DLRATopology topology) {
        LOG.info("Initializing anomaly detector for DLRA topology: " + topology.getName());
        
        for (MicroserviceComponent component : topology.getComponents()) {
            String componentId = component.getId();
            performanceHistories.put(componentId, new PerformanceHistory(
                                    config.getMaxHistoryWindows()));
            
            LOG.info("Added performance history tracking for component: " + componentId);
        }
    }
    
    /**
     * Detect anomalies in DLRA microservices by analyzing tail latency distributions
     * @return List of microservice components with anomalies
     */
    public List<MicroserviceComponent> detectAnomalies(DLRATopology topology) {
        LOG.info("Detecting anomalies for DLRA: " + topology.getName());
        List<MicroserviceComponent> anomalies = new ArrayList<>();
        
        // Collect current performance metrics
        Map<String, PerformanceMetrics> currentMetrics = metricsCollector.collectMetrics(topology);
        
        // Analyze each component for anomalies
        for (MicroserviceComponent component : topology.getComponents()) {
            String componentId = component.getId();
            PerformanceMetrics metrics = currentMetrics.get(componentId);
            
            if (metrics == null) {
                LOG.warn("No metrics available for component: " + componentId);
                continue;
            }
            
            PerformanceHistory history = performanceHistories.get(componentId);
            if (history == null) {
                LOG.warn("No performance history for component: " + componentId);
                history = new PerformanceHistory(config.getMaxHistoryWindows());
                performanceHistories.put(componentId, history);
            }
            
            // Add current metrics to history
            history.addMetrics(metrics);
            
            // Check if current metrics indicate an anomaly
            if (isAnomaly(history)) {
                LOG.info("Detected anomaly in component: " + componentId);
                anomalies.add(component);
            }
        }
        
        return anomalies;
    }
    
    /**
     * Determine if current performance metrics indicate an anomaly
     * based on standard deviation ratio of tail latency
     */
    private boolean isAnomaly(PerformanceHistory history) {
        if (history.getSize() < 2) {
            // Need at least two measurement periods for comparison
            return false;
        }
        
        double currentStdDev = history.getCurrentStdDev();
        double previousStdDev = history.getPreviousStdDev();
        
        if (previousStdDev == 0) {
            // Avoid division by zero
            return false;
        }
        
        // Calculate standard deviation ratio (theta)
        double theta = currentStdDev / previousStdDev;
        
        LOG.info("Standard deviation ratio (theta): " + theta + 
                 " (threshold: " + config.getAnomalyThreshold() + ")");
        
        // Anomaly detected if theta exceeds threshold
        return theta > config.getAnomalyThreshold();
    }
    
    /**
     * Class to maintain performance history for a microservice component
     */
    private static class PerformanceHistory {
        private final int maxHistorySize;
        private final LinkedList<PerformanceMetrics> metricsHistory;
        
        public PerformanceHistory(int maxHistorySize) {
            this.maxHistorySize = maxHistorySize;
            this.metricsHistory = new LinkedList<>();
        }
        
        public void addMetrics(PerformanceMetrics metrics) {
            metricsHistory.addFirst(metrics);
            if (metricsHistory.size() > maxHistorySize) {
                metricsHistory.removeLast();
            }
        }
        
        public int getSize() {
            return metricsHistory.size();
        }
        
        public double getCurrentStdDev() {
            if (metricsHistory.isEmpty()) {
                return 0;
            }
            return metricsHistory.getFirst().getTailLatencyStdDev();
        }
        
        public double getPreviousStdDev() {
            if (metricsHistory.size() < 2) {
                return 0;
            }
            return metricsHistory.get(1).getTailLatencyStdDev();
        }
    }
}
