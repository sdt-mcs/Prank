package org.apache.hadoop.yarn.prank.resource;

import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.containermanagement.ContainerManager;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Schedules the gradual release of resources back to containers
 * after performance anomalies have been resolved
 */
public class ResourceReleaseScheduler {
    private static final Log LOG = LogFactory.getLog(ResourceReleaseScheduler.class);
    private final ContainerManager containerManager;
    private final ScheduledExecutorService scheduler;
    private final Map<ContainerId, Resource> originalResources;
    private final Map<ContainerId, ReleaseStatus> releaseStatuses;
    
    // Default release parameters
    private static final float DEFAULT_RELEASE_INCREMENT = 0.1f;  // 10% increment
    private static final int DEFAULT_RELEASE_ITERATIONS = 5;      // 5 steps to full release
    
    public ResourceReleaseScheduler(ContainerManager containerManager) {
        this.containerManager = containerManager;
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.originalResources = new ConcurrentHashMap<>();
        this.releaseStatuses = new ConcurrentHashMap<>();
    }
    
    /**
     * Schedule gradual resource release for containers
     * @param adjustments Container resource adjustments to release
     * @param intervalMs Time interval between release operations
     */
    public void scheduleRelease(List<ContainerAdjustment> adjustments, long intervalMs) {
        LOG.info("Scheduling gradual resource release for " + adjustments.size() + 
                 " containers with interval " + intervalMs + "ms");
        
        // Store original container resources
        for (ContainerAdjustment adjustment : adjustments) {
            ContainerId containerId = adjustment.getContainerId();
            Container container = containerManager.getContainer(containerId);
            
            if (container != null) {
                originalResources.put(containerId, container.getResource());
                releaseStatuses.put(containerId, new ReleaseStatus(
                                   adjustment.getResource(),  // limited resource
                                   container.getResource(),   // original resource
                                   DEFAULT_RELEASE_INCREMENT,
                                   DEFAULT_RELEASE_ITERATIONS));
                
                LOG.info("Stored original resources for container " + containerId + 
                         ": " + container.getResource());
            }
        }
        
        // Schedule periodic resource release check
        scheduler.scheduleAtFixedRate(new ReleaseTask(), 
                                     intervalMs, 
                                     intervalMs, 
                                     TimeUnit.MILLISECONDS);
    }
    
    /**
     * Task to check and release resources gradually
     */
    private class ReleaseTask implements Runnable {
        @Override
        public void run() {
            try {
                performResourceRelease();
            } catch (Exception e) {
                LOG.error("Error during resource release", e);
            }
        }
    }
    
    /**
     * Perform resource release for containers if conditions are met
     */
    private void performResourceRelease() {
        // Check performance status and gradually release resources
        List<ContainerAdjustment> releaseAdjustments = new ArrayList<>();
        
        for (Map.Entry<ContainerId, ReleaseStatus> entry : releaseStatuses.entrySet()) {
            ContainerId containerId = entry.getKey();
            ReleaseStatus status = entry.getValue();
            
            // Check if container still exists
            Container container = containerManager.getContainer(containerId);
            if (container == null) {
                LOG.info("Container " + containerId + " no longer exists, removing from release tracking");
                releaseStatuses.remove(containerId);
                originalResources.remove(containerId);
                continue;
            }
            
            // Check if performance is stable and resources can be released
            if (canReleaseResources(containerId)) {
                Resource currentResource = container.getResource();
                Resource targetResource = status.calculateNextResource();
                
                LOG.info("Releasing resources for container " + containerId + 
                         " from " + currentResource + " to " + targetResource);
                
                releaseAdjustments.add(new ContainerAdjustment(containerId, targetResource));
                
                // Update release status
                status.incrementReleaseStep();
                
                // If fully released, remove from tracking
                if (status.isFullyReleased()) {
                    LOG.info("Container " + containerId + " resources fully released");
                    releaseStatuses.remove(containerId);
                    originalResources.remove(containerId);
                }
            }
        }
        
        // Apply release adjustments
        if (!releaseAdjustments.isEmpty()) {
            containerManager.updateContainers(releaseAdjustments);
        }
    }
    
    /**
     * Check if resources can be released for a container
     */
    private boolean canReleaseResources(ContainerId containerId) {
        // Check performance status to determine if resources can be released
        // This depends on whether DLRAs are still experiencing performance issues
        // For simplicity, we'll use a random approach here, but in practice
        // this would check actual performance metrics
        
        boolean performanceStable = containerManager.isDLRAPerformanceStable();
        if (performanceStable) {
            LOG.info("DLRA performance is stable, can release resources for container " + containerId);
        } else {
            LOG.info("DLRA performance is not stable, delaying resource release for container " + containerId);
        }
        
        return performanceStable;
    }
    
    /**
     * Status for tracking resource release progress
     */
    private static class ReleaseStatus {
        private final Resource limitedResource;
        private final Resource originalResource;
        private final float releaseIncrement;
        private final int totalSteps;
        private int currentStep;
        
        public ReleaseStatus(Resource limitedResource, Resource originalResource,
                           float releaseIncrement, int totalSteps) {
            this.limitedResource = limitedResource;
            this.originalResource = originalResource;
            this.releaseIncrement = releaseIncrement;
            this.totalSteps = totalSteps;
            this.currentStep = 0;
        }
        
        public Resource calculateNextResource() {
            // Calculate next resource level based on current step
            float progressFactor = Math.min(1.0f, 
                               (float)(currentStep + 1) / totalSteps);
            
            // Interpolate between limited and original resource
            int memory = limitedResource.getMemory() + 
                        (int)((originalResource.getMemory() - limitedResource.getMemory()) 
                              * progressFactor);
            
            int vcores = limitedResource.getVirtualCores() + 
                        (int)((originalResource.getVirtualCores() - limitedResource.getVirtualCores()) 
                              * progressFactor);
            
            return Resource.newInstance(memory, vcores);
        }
        
        public void incrementReleaseStep() {
            currentStep++;
        }
        
        public boolean isFullyReleased() {
            return currentStep >= totalSteps;
        }
    }
}
