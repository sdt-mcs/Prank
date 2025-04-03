package org.apache.hadoop.yarn.prank.anomaly;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * Represents a microservice component identified as a potential source
 * of performance anomalies in a DLRA
 */
public class MicroserviceAnomalySource {
    private final String componentId;
    private final double importanceScore;
    private List<ContainerId> containerIds;
    
    /**
     * Create a new microservice anomaly source
     * @param componentId ID of the microservice component
     * @param importanceScore Importance score from root cause analysis
     */
    public MicroserviceAnomalySource(String componentId, double importanceScore) {
        this.componentId = componentId;
        this.importanceScore = importanceScore;
        this.containerIds = new ArrayList<>();
    }
    
    /**
     * Get the component ID
     * @return Component ID
     */
    public String getComponentId() {
        return componentId;
    }
    
    /**
     * Get the importance score (higher score indicates higher likelihood
     * of being a root cause)
     * @return Importance score
     */
    public double getImportanceScore() {
        return importanceScore;
    }
    
    /**
     * Get the container IDs associated with this component
     * @return List of container IDs
     */
    public List<ContainerId> getContainerIds() {
        return containerIds;
    }
    
    /**
     * Set the container IDs for this component
     * @param containerIds List of container IDs
     */
    public void setContainerIds(List<ContainerId> containerIds) {
        this.containerIds = containerIds;
    }
    
    @Override
    public String toString() {
        return "MicroserviceAnomalySource[componentId=" + componentId + 
               ", importanceScore=" + importanceScore + 
               ", containers=" + containerIds.size() + "]";
    }
}
