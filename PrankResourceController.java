package org.apache.hadoop.yarn.prank.resource;

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.prank.anomaly.MicroserviceAnomalySource;
import org.apache.hadoop.yarn.prank.config.PrankConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.containermanagement.ContainerManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * Priority-driven progressive resource controller for Prank framework
 * Implements the core resource adjustment logic for microservice anomalies
 */
public class PrankResourceController {
    private static final Log LOG = LogFactory.getLog(PrankResourceController.class);
    private final NodeManager nodeManager;
    private final ContainerManager containerManager;
    private final ResourceReleaseScheduler releaseScheduler;
    private final PrankConfiguration config;
    
    // Resource thresholds for different priority levels
    private static final double HIGH_PRIORITY_RESOURCE_FACTOR = 0.9;
    private static final double MED_PRIORITY_RESOURCE_FACTOR = 0.75;
    private static final double LOW_PRIORITY_RESOURCE_FACTOR = 0.6;
    
    public PrankResourceController(NodeManager nodeManager, 
                                  ContainerManager containerManager,
                                  PrankConfiguration config) {
        this.nodeManager = nodeManager;
        this.containerManager = containerManager;
        this.config = config;
        this.releaseScheduler = new ResourceReleaseScheduler(containerManager);
    }
    
    /**
     * Adjust resources based on root cause analysis results
     * @param anomalySources Ranked list of microservices identified as anomaly sources
     * @return Number of affected containers
     */
    public int adjustResourcesForAnomalySources(List<MicroserviceAnomalySource> anomalySources) {
        LOG.info("Adjusting resources for " + anomalySources.size() + " anomaly sources");
        
        // Get nodes hosting the anomalous microservices
        Set<NodeId> criticalNodes = new HashSet<>();
        for (MicroserviceAnomalySource source : anomalySources) {
            List<ContainerId> containers = source.getContainerIds();
            for (ContainerId containerId : containers) {
                NodeId nodeId = containerManager.getContainerNode(containerId);
                if (nodeId != null) {
                    criticalNodes.add(nodeId);
                    LOG.info("Added critical node " + nodeId + " hosting container " + containerId);
                }
            }
        }
        
        // No critical nodes found
        if (criticalNodes.isEmpty()) {
            LOG.warn("No critical nodes found for anomaly sources");
            return 0;
        }
        
        // Calculate priorities for batch containers
        Map<ContainerId, Float> containerPriorities = calculateBatchContainerPriorities();
        
        // Apply resource adjustments
        int adjustedCount = 0;
        for (NodeId nodeId : criticalNodes) {
            List<ContainerAdjustment> adjustments = calculateNodeAdjustments(
                    nodeId, containerPriorities);
            
            if (!adjustments.isEmpty()) {
                LOG.info("Applying " + adjustments.size() + " resource adjustments on node " + nodeId);
                
                // Apply resource limitations
                containerManager.updateContainers(adjustments);
                
                // Schedule gradual resource release
                releaseScheduler.scheduleRelease(adjustments, config.getAdjustmentIntervalMs());
                
                adjustedCount += adjustments.size();
            }
        }
        
        return adjustedCount;
    }
    
    /**
     * Calculate resource adjustments for a specific node
     */
    private List<ContainerAdjustment> calculateNodeAdjustments(
            NodeId nodeId, Map<ContainerId, Float> containerPriorities) {
        
        List<ContainerAdjustment> adjustments = new ArrayList<>();
        List<Container> batchContainers = getBatchContainersOnNode(nodeId);
        
        if (batchContainers.isEmpty()) {
            return adjustments;
        }
        
        // Sort containers by priority (lower = more suitable for limitation)
        Collections.sort(batchContainers, new Comparator<Container>() {
            @Override
            public int compare(Container c1, Container c2) {
                float p1 = containerPriorities.getOrDefault(c1.getId(), 1.0f);
                float p2 = containerPriorities.getOrDefault(c2.getId(), 1.0f);
                return Float.compare(p1, p2);
            }
        });
        
        // Determine resources to reclaim based on node pressure
        NodeResourceStatus nodeStatus = nodeManager.getNodeResourceStatus(nodeId);
        Resource targetReclaim = calculateTargetResources(nodeStatus);
        Resource reclaimedSoFar = Resource.newInstance(0, 0);
        
        LOG.info("Target resource reclamation for node " + nodeId + ": " + targetReclaim);
        
        // Apply progressive limitations to batch containers
        for (Container container : batchContainers) {
            // Stop if we've reclaimed enough resources
            if (ResourceCalculator.compare(reclaimedSoFar, targetReclaim) >= 0) {
                LOG.info("Sufficient resources reclaimed, stopping adjustment process");
                break;
            }
            
            // Calculate adjusted allocation for this container
            Resource originalAlloc = container.getResource();
            Resource newAlloc;
            
            // Apply different limiting factors based on priority
            float priority = containerPriorities.getOrDefault(container.getId(), 1.0f);
            if (priority < 0.3f) {
                // Low priority - more aggressive reduction
                newAlloc = Resources.multiply(originalAlloc, LOW_PRIORITY_RESOURCE_FACTOR);
                LOG.info("Low priority container " + container.getId() + 
                         ", reduction factor: " + LOW_PRIORITY_RESOURCE_FACTOR);
            } else if (priority < 0.7f) {
                // Medium priority - moderate reduction
                newAlloc = Resources.multiply(originalAlloc, MED_PRIORITY_RESOURCE_FACTOR);
                LOG.info("Medium priority container " + container.getId() + 
                         ", reduction factor: " + MED_PRIORITY_RESOURCE_FACTOR);
            } else {
                // High priority - minimal reduction
                newAlloc = Resources.multiply(originalAlloc, HIGH_PRIORITY_RESOURCE_FACTOR);
                LOG.info("High priority container " + container.getId() + 
                         ", reduction factor: " + HIGH_PRIORITY_RESOURCE_FACTOR);
            }
            
            // Ensure minimum resources
            newAlloc = enforceMinimumResources(newAlloc);
            
            // Update reclaimed resources tracking
            Resource reclaimed = Resources.subtract(originalAlloc, newAlloc);
            reclaimedSoFar = Resources.add(reclaimedSoFar, reclaimed);
            
            LOG.info("Adjusting container " + container.getId() + " from " + 
                     originalAlloc + " to " + newAlloc + " (reclaimed: " + reclaimed + ")");
            
            // Add to adjustment list
            adjustments.add(new ContainerAdjustment(container.getId(), newAlloc));
        }
        
        return adjustments;
    }
    
    /**
     * Calculate priorities for batch containers based on job attributes
     */
    private Map<ContainerId, Float> calculateBatchContainerPriorities() {
        Map<ContainerId, Float> priorities = new HashMap<>();
        List<Container> allBatchContainers = containerManager.getAllBatchContainers();
        
        for (Container container : allBatchContainers) {
            ContainerId containerId = container.getId();
            ApplicationId appId = containerId.getApplicationAttemptId().getApplicationId();
            
            // Get job attributes
            float deadlineFactor = getJobDeadlineFactor(appId);
            float progressFactor = getJobProgressFactor(appId);
            float importanceFactor = getJobImportanceFactor(appId);
            float resourceFactor = getContainerResourceFactor(container);
            
            // Calculate overall priority
            // Higher value = higher priority = less suitable for limitation
            float priority = (0.2f * deadlineFactor) +
                            (0.3f * importanceFactor) +
                            (0.4f * resourceFactor) +
                            (0.1f * progressFactor);
            
            priorities.put(containerId, priority);
        }
        
        return priorities;
    }
    
    /**
     * Get batch containers running on a specific node
     */
    private List<Container> getBatchContainersOnNode(NodeId nodeId) {
        List<Container> allContainers = nodeManager.getContainersOnNode(nodeId);
        List<Container> batchContainers = new ArrayList<>();
        
        for (Container container : allContainers) {
            if (isBatchContainer(container)) {
                batchContainers.add(container);
            }
        }
        
        return batchContainers;
    }
    
    /**
     * Determine if a container belongs to a batch job
     */
    private boolean isBatchContainer(Container container) {
        // Implementation depends on how batch jobs are identified in the system
        // Typically based on application tags or job type
        ApplicationId appId = container.getId().getApplicationAttemptId().getApplicationId();
        return containerManager.getApplicationType(appId).equals("BATCH");
    }
    
    /**
     * Calculate target resources to reclaim based on node status
     */
    private Resource calculateTargetResources(NodeResourceStatus nodeStatus) {
        // Calculate based on node utilization and threshold
        Resource totalResources = nodeStatus.getCapacity();
        Resource usedResources = nodeStatus.getUsed();
        
        // Calculate utilization percentage
        double cpuUtilization = (double) usedResources.getVirtualCores() / 
                               totalResources.getVirtualCores();
        double memUtilization = (double) usedResources.getMemory() / 
                               totalResources.getMemory();
        
        // Target reclamation based on utilization
        double cpuReclaimFactor = calculateReclaimFactor(cpuUtilization);
        double memReclaimFactor = calculateReclaimFactor(memUtilization);
        
        // Apply reclamation factors
        int cpuToReclaim = (int) (totalResources.getVirtualCores() * cpuReclaimFactor);
        int memToReclaim = (int) (totalResources.getMemory() * memReclaimFactor);
        
        return Resource.newInstance(memToReclaim, cpuToReclaim);
    }
    
    /**
     * Calculate reclamation factor based on resource utilization
     */
    private double calculateReclaimFactor(double utilization) {
        double reclaimFactor = 0.0;
        double utilizationThreshold = config.getUtilizationThreshold();
        
        if (utilization > utilizationThreshold) {
            // Linear increase in reclamation factor as utilization increases
            reclaimFactor = Math.min(0.3, (utilization - utilizationThreshold) / 
                                    (1.0 - utilizationThreshold) * 0.3);
        }
        
        return reclaimFactor;
    }
    
    /**
     * Enforce minimum resources for containers
     */
    private Resource enforceMinimumResources(Resource resource) {
        int minMemory = config.getMinContainerMemoryMB();
        int minCpu = config.getMinContainerVCores();
        
        int memory = Math.max(resource.getMemory(), minMemory);
        int cpu = Math.max(resource.getVirtualCores(), minCpu);
        
        return Resource.newInstance(memory, cpu);
    }
    
    /**
     * Get deadline factor for a job (higher = closer to deadline)
     */
    private float getJobDeadlineFactor(ApplicationId appId) {
        long currentTime = System.currentTimeMillis();
        long deadline = containerManager.getJobDeadline(appId);
        
        if (deadline <= 0) {
            return 0.5f; // Default value for jobs without deadline
        }
        
        long timeRemaining = deadline - currentTime;
        if (timeRemaining <= 0) {
            return 1.0f; // Past deadline
        }
        
        // Normalize: closer to deadline = higher factor
        long jobDuration = containerManager.getJobDuration(appId);
        return Math.min(1.0f, Math.max(0.0f, 
               1.0f - ((float) timeRemaining / jobDuration)));
    }
    
    /**
     * Get job progress factor (higher = more complete)
     */
    private float getJobProgressFactor(ApplicationId appId) {
        float progress = containerManager.getJobProgress(appId);
        return Math.min(1.0f, Math.max(0.0f, progress));
    }
    
    /**
     * Get job importance factor based on business priority
     */
    private float getJobImportanceFactor(ApplicationId appId) {
        int priority = containerManager.getJobPriority(appId);
        // Normalize to 0-1 range (assuming priority ranges from 0 to MAX_PRIORITY)
        return Math.min(1.0f, Math.max(0.0f, 
               (float) priority / containerManager.getMaxJobPriority()));
    }
    
    /**
     * Get resource factor for a container (higher = more resource intensive)
     */
    private float getContainerResourceFactor(Container container) {
        Resource resources = container.getResource();
        Resource clusterAverage = containerManager.getAverageContainerResources();
        
        float memoryFactor = (float) resources.getMemory() / clusterAverage.getMemory();
        float cpuFactor = (float) resources.getVirtualCores() / clusterAverage.getVirtualCores();
        
        // Average of memory and CPU factors
        return (memoryFactor + cpuFactor) / 2.0f;
    }
}
