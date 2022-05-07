package org.apache.hadoop.hdfs.serverless.operation;

import io.hops.events.*;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.InvalidationDataAccess;
import io.hops.metadata.hdfs.dal.WriteAcknowledgementDataAccess;
import io.hops.metadata.hdfs.entity.Invalidation;
import io.hops.metadata.hdfs.entity.WriteAcknowledgement;
import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.EntityContext;
import io.hops.transaction.context.INodeContext;
import io.hops.transaction.handler.TransactionalRequestHandler;
import io.hops.transaction.lock.HdfsTransactionalLockAcquirer;
import io.hops.transaction.lock.TransactionLockAcquirer;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.OpenWhiskHandler;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZKClient;
import org.apache.hadoop.hdfs.serverless.zookeeper.ZooKeeperInvalidation;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Encapsulates the execution of the consistency protocol. This class extends {@link Thread} because the protocol
 * is meant to be executed in a separate thread. This is because of how ClusterJ/NDB works
 * (the {@link io.hops.metadata.ndb.wrapper.HopsSession} objects are not supposed to be shared between threads, and
 * transactions running on the thread invoking the consistency protocol.
 */
public class ConsistencyProtocol extends Thread implements HopsEventListener {
    /**
     * Basically just exists to make debugging/performance testing easier. I use this to dynamically
     * enable or disable the consistency protocol, which is used during write transactions.
     */
    public static boolean DO_CONSISTENCY_PROTOCOL = true;

    private static Log LOG = LogFactory.getLog(ConsistencyProtocol.class);

    /**
     * This flag is updated by the end of the consistency protocol. If true, then the calling thread can continue
     * with whatever operation they were trying to perform. If false, then the operation should be aborted.
     */
    private volatile boolean canProceed;

    /**
     * Exceptions encountered while executing the consistency protocol.
     */
    private volatile ArrayList<Exception> exceptions = new ArrayList<>();

    /**
     * Required to determine the INodes (metadata) involves in this execution of the Consistency Protocol. We need
     * this information in order to determine which deployments will receive INVs.
     */
    private EntityContext<?> callingThreadINodeContext;

    /**
     * Alternatively, the calling thread can pass in a set specifying the involved deployments. This is performed
     * for subtree invalidations/instances of the consistency protocol performed for subtree operations.
     *
     * This is a set of IDs denoting deployments from which we require ACKs. Our own deployment will always be
     * involved. Other deployments may be involved during subtree operations and when creating new directories,
     * as these types of operations modify INodes from multiple deployments.
     */
    private Set<Integer> involvedDeployments;

    /**
     * Used to record metrics about the execution of the consistency protocol.
     */
    private final TransactionAttempt transactionAttempt;

    /**
     * The time at which the transaction associated with the consistency protocol began.
     */
    private final long transactionStartTime;

    private final ServerlessNameNode serverlessNameNodeInstance;

    /**
     * Used to keep track of whether an ACK has been received from each follower NN during the consistency protocol.
     */
    private HashSet<Long> waitingForAcks = new HashSet<>();

    /**
     * Mapping from deployment number to the set of NN IDs, representing the set of ACKs we're still waiting on
     * for that deployment.
     */
    private HashMap<Integer, Set<Long>> waitingForAcksPerDeployment = new HashMap<>();

    /**
     * HashMap from NameNodeID to deployment number, as we can't really recovery deployment number for a given
     * event in the event-received handler. But we can if we note which deployment a given NN is from, because
     * events contain NameNodeID information.
     */
    private HashMap<Long, Integer> nameNodeIdToDeploymentNumberMapping = new HashMap<>();

    /**
     * We use this CountDownLatch when waiting on ACKs and watching for changes in membership. Specifically,
     * each time we receive an ACK, the latch is decremented, and if any follower NNs leave the group during
     * this operation, the latch is also decremented. Thus, we are eventually woken up when the CountDownLatch
     * reaches zero.
     */
    private CountDownLatch countDownLatch;

    /**
     * Used to keep track of write ACKs required from each deployment. Normally, we only require ACKs from our own
     * deployment; however, we may require ACKs from other deployments during subtree operations and when creating
     * new directories.
     */
    private Map<Integer, List<WriteAcknowledgement>> writeAcknowledgementsMap;

    /**
     * Watchers we create to observe membership changes on other deployments.
     *
     * We remove these when we leave the deployment.
     */
    private final HashMap<Integer, Watcher> watchers;

    /**
     * Used as a unique identifier for the operation. This is only used during write operations.
     */
    protected final long operationId;

    /**
     * Used for event-style data collection of transaction timings.
     */
    private final TransactionEvent transactionEvent;

    /**
     * If true, use ZooKeeper for ACKs and INVs. Otherwise, use the hops-metadata-dal.
     */
    private final boolean useZooKeeperForACKsAndINVs;

    /**
     * If true, then this is being executed as part of a subtree operation.
     * Otherwise, this is just a normal write operation.
     */
    private final boolean subtreeOperation;

    /**
     * If this is a subtree operation, then the root of the subtree must be specified, as the root is used
     * by follower NNs to invalidate all cached INodes within the subtree.
     */
    private final String subtreeRoot;

    /**
     * We can run part of the consistency protocol in the original, calling thread. Specifically, we run Step 0,
     * which involves computing how many ACKs we need to create. In some cases, we do not need to create any ACKs,
     * and we can avoid the overhead of starting the Consistency Protocol thread if we see that we will not be
     * creating/writing any ACKs.
     */
    private boolean totalAcksWerePreComputed = false;

    /**
     * This is computed during the "pre-computation" phase.
     */
    private int totalNumberOfACKsRequiredPreComputed = -1;

    private Collection<INode> invalidatedINodes;

    /**
     * Constructor for non-subtree operations.
     *
     * @param callingThreadINodeContext The INode {@link EntityContext} object of the calling thread. We need this
     *                                  object in order to determine the metadata involved in this consistency
     *                                  protocol, as this determines which deployments we issue INVs to.
     * @param involvedDeployments       Set of deployments involved in the subtree operation. This is calculated
     *                                  when obtaining database locks while walking through the subtree at the
     *                                  beginning of the subtree protocol. Alternatively, we may wish to compute this
     *                                  pre-emptively to determine if the protocol will be executed or not. This would
     *                                  be done BEFORE starting the Consistency Protocol thread, thereby avoiding the
     *                                  overhead of starting the thread if we're going to end up aborting the protocol
     *                                  early anyway.
     * @param transactionAttempt        Used for tracking metrics about this particular transaction attempt.
     * @param transactionEvent          Used for tracking metrics about the overall transaction.
     * @param transactionStartTime      The time at which the transaction started.
     * @param useZooKeeper              If true, use ZooKeeper for ACKs and INVs. Otherwise, use the hops-metadata-dal.
     * @param invalidatedINodes         The INodes that are being invalidated by this transaction. If this is null,
     *                                  then this will be computed during the execution of the protocol.
     */
    public ConsistencyProtocol(EntityContext<?> callingThreadINodeContext,
                               Set<Integer> involvedDeployments,
                               TransactionAttempt transactionAttempt,
                               TransactionEvent transactionEvent,
                               long transactionStartTime,
                               boolean useZooKeeper,
                               boolean subtreeOperation,
                               String subtreeRoot,
                               Collection<INode> invalidatedINodes) {
        this.callingThreadINodeContext = callingThreadINodeContext;
        this.involvedDeployments = involvedDeployments;
        this.transactionAttempt = transactionAttempt;
        this.transactionEvent = transactionEvent;
        this.transactionStartTime = transactionStartTime;
        this.serverlessNameNodeInstance = ServerlessNameNode.tryGetNameNodeInstance(true);
        this.watchers = new HashMap<>();
        this.operationId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        this.useZooKeeperForACKsAndINVs = useZooKeeper;
        this.subtreeOperation = subtreeOperation;
        this.subtreeRoot = subtreeRoot;
        this.invalidatedINodes = invalidatedINodes;
    }

    /**
     * Pre-compute the total number of ACKs. This also computes a lot of state used later in the consistency
     * protocol. Specifically, this populates the 'writeAcknowledgementsMap' map, the
     * 'waitingForAcksPerDeployment' map, the 'nameNodeIdToDeploymentNumberMapping' map, and the 'waitingForAcks'
     * map. This also updates the 'involvedDeployments' set by removing from 'involvedDeployments' any deployment
     * for which we do not actually need any ACKs.
     */
    public int precomputeAcks() throws IOException {
        LOG.debug("Pre-computing ACKs (before running full consistency protocol).");
        if (this.invalidatedINodes == null) {
            LOG.error("Cannot pre-compute ACKs if set of invalidated INodes is null.");
            return -1;
        }

        // We should always need to compute the set of involved deployments during this "pre-compute ACKs" step.
        // The set of involved deployments is only ever passed-in during subtree operations, and we do not perform
        // this "pre-compute ACKs" step during subtree operations.
        if (this.involvedDeployments == null) {
            long s = System.currentTimeMillis();
            computeInvolvedDeployments();
            long t1 = System.currentTimeMillis();

            if (LOG.isTraceEnabled()) LOG.trace("Computed involved deployments in " + (t1 - s) + " ms.");
        }

        long s = System.currentTimeMillis();
        try {
            this.totalNumberOfACKsRequiredPreComputed = computeAckRecords(transactionStartTime);
            this.totalAcksWerePreComputed = true;
        } catch (Exception ex) {
            LOG.error("Encountered exception while pre-computing ACKs:", ex);
            throw new IOException(ex);
        }

        if (LOG.isTraceEnabled())
            LOG.trace("Computed ACK records in " + (System.currentTimeMillis() - s) + " ms.");

        return this.totalNumberOfACKsRequiredPreComputed;
    }

    /**
     * Utility function for running an instance of the Consistency Protocol for subtree operations.
     *
     * @param associatedDeployments The deployments involved in the subtree operation. These deployments will require
     *                              an invalidation during the consistency protocol.
     * @param src The source/target directory of the subtree operation (the root of the subtree).
     * @return True if the consistency protocol executed successfully, indicating that the subtree operation
     * should proceed like normal. Otherwise false, which means that the subtree protocol should abort.
     */
    public static boolean runConsistencyProtocolForSubtreeOperation(Set<Integer> associatedDeployments, String src) {
        int numAssociatedDeployments = associatedDeployments.size();

        if (LOG.isDebugEnabled()) {
            LOG.debug("=============== Subtree Consistency Protocol ===============");
            LOG.debug("There " + (numAssociatedDeployments == 1 ? "is 1 deployment " : "are " +
                    associatedDeployments.size() + " deployments ") + " associated with subtree rooted at '" + src + "'.");
            LOG.debug("Associated deployments: " + StringUtils.join(", ", associatedDeployments));
        }

        // This is sort of a dummy ID.
        long transactionId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        long txStartTime = System.currentTimeMillis();

        TransactionAttempt txAttempt = null;
        TransactionEvent txEvent = null;
        if (TransactionalRequestHandler.TX_EVENTS_ENABLED) {
            txAttempt = new TransactionAttempt(0);
            txEvent = new TransactionEvent(transactionId);
            txEvent.setTransactionStartTime(txStartTime);
            txEvent.addAttempt(txAttempt);
        }

        ConsistencyProtocol subtreeConsistencyProtocol = new ConsistencyProtocol(
                null, associatedDeployments, txAttempt, txEvent,
                txStartTime, true, true, src, null);
        subtreeConsistencyProtocol.start();

        boolean interruptedExceptionOccurred = false;

        try {
            subtreeConsistencyProtocol.join();
        } catch (InterruptedException ex) {
            LOG.error("Encountered interrupted exception while joining with subtree consistency protocol:", ex);
            interruptedExceptionOccurred = true;
        }

        long txEndTime = System.currentTimeMillis();
        long txDurationMilliseconds = txEndTime - txStartTime;
        if (TransactionalRequestHandler.TX_EVENTS_ENABLED && txEvent != null) {
            txEvent.setTransactionEndTime(txEndTime);
        }

        if (!subtreeConsistencyProtocol.getCanProceed() || interruptedExceptionOccurred) {
            LOG.error("Subtree Consistency Protocol failed to execute properly. Time elapsed: " +
                    txDurationMilliseconds + " ms. Checking for exceptions...");

            List<Exception> exceptions = subtreeConsistencyProtocol.getExceptions();

            LOG.error("Found " + exceptions.size() + " exception(s) from Subtree Consistency Protocol.");

            int counter = 1;
            for (Exception ex : exceptions) {
                LOG.error("Exception #" + (counter++) + " from Subtree Consistency Protocol:");
                LOG.error(ex);
            }
            return false;
        }

        if (LOG.isDebugEnabled()) LOG.debug("Subtree Consistency Protocol executed successfully in " + txDurationMilliseconds + " ms.");
        return true;
    }

    public boolean getCanProceed() { return this.canProceed; }

    public List<Exception> getExceptions() { return this.exceptions; }

    private void computeInvolvedDeployments() {
//        if (LOG.isDebugEnabled()) LOG.debug("Computing involved deployments as they were not provided to us directly.");
        involvedDeployments = new HashSet<>();

        // If we entered this if-statement's code block, then we should NOT be executing a subtree operation.
        // As a result, we should have access to the set of invalidated INodes for this particular transaction.
        if (invalidatedINodes == null) {
            throw new IllegalStateException(
                    "Set of invalidated INodes is null when it shouldn't be. INode Context object was null: " +
                            (callingThreadINodeContext == null));
        }

        for (INode invalidatedINode : invalidatedINodes) {
            int mappedDeploymentNumber = serverlessNameNodeInstance.getMappedDeploymentNumber(invalidatedINode);
            int localDeploymentNumber = serverlessNameNodeInstance.getDeploymentNumber();

            // We'll have to guest-join the other deployment if the INode is not mapped to our deployment.
            // This is common during subtree operations and when creating a new directory (as that modifies the parent
            // INode of the new directory, which is possibly mapped to a different deployment).
            if (mappedDeploymentNumber != localDeploymentNumber) {
//                if (LOG.isDebugEnabled()) LOG.debug("INode '" + invalidatedINode.getLocalName() +
//                        "' is mapped to a different deployment (" + mappedDeploymentNumber + ").");
                involvedDeployments.add(mappedDeploymentNumber);
            }
        }
    }

    @Override
    public void run() {
        //// // // // // // // // // // ////
        // CURRENT CONSISTENCY PROTOCOL   //
        //// // // // // // // // // // ////
        //
        // WARNING:
        // This description is slightly out-dated, as we now use ZooKeeper to store ACKs and INVs.
        // The general steps and order of operations remains unchanged, however. Instead of storing ACKs
        // and INVs as entries in an NDB table, we use ephemeral ZNodes stored in ZooKeeper. Just as each
        // deployment had its own ACK and INV table in NDB, each deployment has an associated persistent ZNode
        // for ACKs and another for INVs.
        //
        // TERMINOLOGY:
        // - Leader NameNode: The NameNode performing the write operation.
        // - Follower NameNode: NameNode instance from the same deployment as the Leader NameNode.
        //
        // The updated consistency protocol for Serverless NameNodes is as follows:
        // (1) The Leader NN begins listening for changes in group membership from ZooKeeper.
        //     The Leader will also subscribe to events on the ACKs table for reasons that will be made clear shortly. We
        //     need to subscribe first to ensure we receive notifications from follower NNs ACK'ing the entries.
        //     IMPORTANT: Another reason we must subscribe BEFORE adding the ACKs is that, if we add our ACKs first, then
        //     another NN could issue an INV that causes one of our followers to check for ACKs. They see the ACKs we just
        //     added and ACK them, but we haven't subscribed yet! So, we miss the notification, and our protocol fails as
        //     a result. Thus, we must subscribe BEFORE adding any ACKs to intermediate storage.
        // (2) The Leader NN adds N-1 un-ACK'd records to the "ACKs" table of the Leader's deployment, where N is the
        //     number of nodes in the Leader's deployment. (N-1 as it does not need to add a record for itself.)
        // (3) The leader issues one INV per modified INode to the target deployment's INV table.
        // (4) Follower NNs will ACK their entry in the ACKs table upon receiving the INV from intermediate storage (NDB).
        //     The follower will also invalidate its cache at this point, thereby readying itself for the upcoming write.
        // (5) The Leader listens for updates on the ACK table, waiting for all entries to be ACK'd.
        //     If there are any NN failures during this phase, the Leader will detect them via ZK. The Leader does not
        //     need ACKs from failed NNs, as they invalidate their cache upon returning.
        // (6) Once all ACKs are received, the leader continues with the operation.
        //
        //// // // // // // // // // // //// // // // // // // // // // //// // // // // // // // // // ////

        if (!DO_CONSISTENCY_PROTOCOL) {
            if (LOG.isDebugEnabled()) LOG.debug("Skipping consistency protocol as 'DO_CONSISTENCY_PROTOCOL' is set to false.");
            canProceed = true;
            return;
        }

        // Technically this isn't true yet, but we'll need to unsubscribe after the call to `subscribeToAckEvents()`.
        boolean needToUnsubscribe = true;
        long startTime = System.currentTimeMillis();

        // This block of code is only relevant/executed when we're running the consistency protocol for non-subtree
        // operations. When we are running a subtree operation, the 'callingThreadINodeContext' variable will be null.
        // We don't care about the set of invalidated INodes during a subtree operation, also. We just don't factor
        // that into things. I think it's the case that there will always be at least one invalidated INode anyway
        // in that of the subtree root.
        if (invalidatedINodes == null && !subtreeOperation) {
            INodeContext transactionINodeContext = (INodeContext) callingThreadINodeContext;
            invalidatedINodes = transactionINodeContext.getInvalidatedINodes();
            int numInvalidated = invalidatedINodes.size();

            // If there are no invalidated INodes, then we do not need to carry out the consistency protocol;
            // however, if there is at least 1 invalidated INode, then we must proceed with the protocol.
            if (numInvalidated == 0) {
                this.canProceed = true;
                return;
            }
        }

        if (LOG.isDebugEnabled()) LOG.debug("=-=-=-=-= CONSISTENCY PROTOCOL =-=-=-=-=");
        if (LOG.isDebugEnabled()) LOG.debug("Operation ID: " + operationId);
        if (LOG.isDebugEnabled()) LOG.debug("Operation Start Time: " + transactionStartTime);

        // Sanity check. Make sure we have a valid reference to the ServerlessNameNode. This isn't the cleanest, but
        // with the way HopsFS has structured its code, this is workable for our purposes.
        if (serverlessNameNodeInstance == null)
            throw new IllegalStateException(
                    "Somehow a Transaction is occurring when the static ServerlessNameNode instance is null.");

        // transactionEvent.setRequestId(serverlessNameNodeInstance.getRequestCurrentlyProcessing());
        if (TransactionalRequestHandler.TX_EVENTS_ENABLED)
            transactionEvent.setRequestId("UNKNOWN");

        // NOTE: The local deployment will NOT always be involved now that the subtree protocol uses this same code.
        //       Before the subtree protocol used this code, the only NNs that could modify an INode were those from
        //       the mapped deployment. As a result, the Leader NN's deployment would always be involved. But now that the
        //       subtree protocol uses this code, the Leader may not be modifying an INode from its own deployment. So, we
        //       do not automatically add the local deployment to the set of involved deployments. If the local deployment
        //       needs to be invalidated, then it will be added when we see that a modified INode is mapped to it.
        //
        // Keep track of all deployments involved in this transaction.
        if (involvedDeployments == null)
            computeInvolvedDeployments();

        if (LOG.isDebugEnabled()) LOG.debug("Leader NameNode: " + serverlessNameNodeInstance.getFunctionName() + ", ID = "
                + serverlessNameNodeInstance.getId() + ", Follower NameNodes: "
                + serverlessNameNodeInstance.getActiveNameNodes().getActiveNodes().toString() + ".");

        long preprocessingEndTime = System.currentTimeMillis();
        if (TransactionalRequestHandler.TX_EVENTS_ENABLED)
            transactionAttempt.setConsistencyPreprocessingTimes(startTime, preprocessingEndTime);

        // ======================================
        // === EXECUTING CONSISTENCY PROTOCOL ===
        // ======================================

        // OPTIMIZATION: Pre-calculate the number of write ACK records that we'll be adding to intermediate storage.
        // If this value is zero, we do not need to subscribe to ACK events.
        //
        // As an optimization, we first calculate how many ACKs we're going to need. If we find that this value is 0,
        // then we do not bother subscribing to ACK events. But if there is at least one ACK, then we subscribe first
        // before adding the ACKs to NDB. We need to be listening for ACK events before the events are added so that
        // we do not miss any notifications.
        int totalNumberOfACKsRequired;

        if (totalAcksWerePreComputed) {
            totalNumberOfACKsRequired = totalNumberOfACKsRequiredPreComputed;
        } else {
            try {
                // Pass the set of additional deployments we needed to join, as we also need ACKs from those deployments.
                totalNumberOfACKsRequired = computeAckRecords(transactionStartTime);
            } catch (Exception ex) {
                LOG.error("Exception encountered while computing/creating ACK records in-memory:", ex);
                this.canProceed = false;
                return;
            }
        }

        long computeAckRecordsEndTime = System.currentTimeMillis();

        if (TransactionalRequestHandler.TX_EVENTS_ENABLED)
            transactionAttempt.setConsistencyComputeAckRecordTimes(preprocessingEndTime, computeAckRecordsEndTime);

        if (LOG.isDebugEnabled()) LOG.debug("Created ACK records in " + (computeAckRecordsEndTime - preprocessingEndTime) + " ms.");

        // =============== STEP 1 ===============
        //
        // We only need to perform these steps of the protocol if the total number of ACKs required is at least one.
        // As an optimization, we split Step 1 into two parts. In the first part, we simply create the ACK records
        // in-memory. Sometimes we won't create any. In that case, we can essentially skip the entire protocol. If we
        // create at least one ACK record in-memory however, then we must first subscribe to ACK events BEFORE adding
        // the newly-created ACK records to intermediate storage. Once we've subscribed to ACK events, we'll write the
        // ACK records to NDB. Doing things in this order eliminates the chance that we miss a notification.
        if (totalNumberOfACKsRequired > 0) {
            // Now that we've added ACKs based on the current membership of the group, we'll join the deployment as a guest
            // and begin monitoring for membership changes. Since we already added ACKs for every active instance, we aren't
            // going to miss any. We DO need to double-check that nobody dropped between when we first queried the deployments
            // for their membership and when we begin listening for changes, though. (We do the same for our own deployment.)
            // joinOtherDeploymentsAsGuest();

            long joinDeploymentsEndTime = System.currentTimeMillis();

            if (TransactionalRequestHandler.TX_EVENTS_ENABLED)
                transactionAttempt.setConsistencyJoinDeploymentsTimes(computeAckRecordsEndTime, joinDeploymentsEndTime);

            try {
                // Since there's at least 1 ACK record, we subscribe to ACK events.
                // We have not written any ACKs to NDB yet.
                if (!useZooKeeperForACKsAndINVs)
                    subscribeToAckEvents(); // We only do this for NDB. Not when using ZooKeeper.

                long subscribeToEventsEndTime = System.currentTimeMillis();

                if (TransactionalRequestHandler.TX_EVENTS_ENABLED)
                    transactionAttempt.setConsistencySubscribeToAckEventsTimes(joinDeploymentsEndTime, subscribeToEventsEndTime);
            } catch (InterruptedException | StorageException e) {
                LOG.error("Encountered error while waiting on event manager to create event subscription:", e);
                exceptions.add(e);
                this.canProceed = false;
                return;
            }

            // =============== STEP 2 ===============
            //
            // Now that we've subscribed to ACK events, we can add our ACKs to the table.
            if (LOG.isTraceEnabled()) LOG.trace("=-----=-----= Step 2 - Writing ACK Records to Intermediate Storage =-----=-----=");
            TransactionLockAcquirer locksAcquirer = new HdfsTransactionalLockAcquirer(); // Only used with NDB.
            if (useZooKeeperForACKsAndINVs) {
                try {
                    long invStartTime = System.currentTimeMillis();
                    issueInvalidationsZooKeeper(invalidatedINodes);
                    long invEndTime = System.currentTimeMillis();

                    if (TransactionalRequestHandler.TX_EVENTS_ENABLED)
                        transactionAttempt.setConsistencyIssueInvalidationsTimes(invStartTime, invEndTime);
                } catch (Exception ex) {
                    LOG.error("Encountered exception while storing INVs in ZooKeeper:", ex);
                    exceptions.add(ex);
                    canProceed = false;
                    return;
                }
            }
            else {
                long writeAcksToStorageStartTime = System.currentTimeMillis();

                WriteAcknowledgementDataAccess<WriteAcknowledgement> writeAcknowledgementDataAccess =
                        (WriteAcknowledgementDataAccess<WriteAcknowledgement>) HdfsStorageFactory.getDataAccess(WriteAcknowledgementDataAccess.class);

                if (LOG.isDebugEnabled()) LOG.debug("Beginning transaction to write ACKs and INVs in single step.");

                try {
                    EntityManager.begin();
                } catch (StorageException e) {
                    e.printStackTrace();
                }

                for (Map.Entry<Integer, List<WriteAcknowledgement>> entry : writeAcknowledgementsMap.entrySet()) {
                    int deploymentNumber = entry.getKey();
                    List<WriteAcknowledgement> writeAcknowledgements = entry.getValue();

                    if (writeAcknowledgements.size() > 0) {
                        if (LOG.isTraceEnabled()) LOG.trace("Adding " + writeAcknowledgements.size()
                                + " ACK entries for deployment #" + deploymentNumber + ".");
                        try {
                            writeAcknowledgementDataAccess.addWriteAcknowledgements(writeAcknowledgements, deploymentNumber);
                        } catch (StorageException e) {
                            LOG.error("Encountered exception while storing ACKs in intermediate storage:", e);
                            exceptions.add(e);
                            canProceed = false;
                            return;
                        }
                    } else {
                        if (LOG.isTraceEnabled()) LOG.trace("0 ACKs required from deployment #" + deploymentNumber + "...");
                    }
                }

                long writeAcksToStorageEndTime = System.currentTimeMillis();

                if (TransactionalRequestHandler.TX_EVENTS_ENABLED)
                    transactionAttempt.setConsistencyWriteAcksToStorageTimes(writeAcksToStorageStartTime, writeAcksToStorageEndTime);

                // =============== STEP 3 ===============
                try {
                    issueInvalidationsNDB(invalidatedINodes, transactionStartTime);
                } catch (StorageException e) {
                    LOG.error("Encountered exception while issuing validations:", e);
                    exceptions.add(e);
                    canProceed = false;
                    return;
                }

                if (LOG.isDebugEnabled()) LOG.debug("Committing transaction containing ACKs and INVs now.");
                TransactionLocks transactionLocks = locksAcquirer.getLocks();

                try {
                    EntityManager.commit(transactionLocks);
                } catch (TransactionContextException | StorageException e) {
                    LOG.error("Encountered exception committing ACKs and INVs:", e);
                    exceptions.add(e);
                    canProceed = false;
                    return;
                }


                if (TransactionalRequestHandler.TX_EVENTS_ENABLED) {
                    long issueInvalidationsEndTime = System.currentTimeMillis();
                    transactionAttempt.setConsistencyIssueInvalidationsTimes(writeAcksToStorageEndTime, issueInvalidationsEndTime);
                }
            }

            long waitForAcksStartTime = System.currentTimeMillis();
            try {
                // =============== STEPS 4 & 5 ===============
                waitForAcks();

                if (TransactionalRequestHandler.TX_EVENTS_ENABLED) {
                    long waitForAcksEndTime = System.currentTimeMillis();
                    transactionAttempt.setConsistencyWaitForAcksTimes(waitForAcksStartTime, waitForAcksEndTime);
                }
            } catch (Exception ex) {
                LOG.error("Exception encountered on Step 4 and 5 of consistency protocol (waiting for ACKs).");
                LOG.error("We're still waiting on " + waitingForAcks.size() +
                        " ACKs from the following NameNodes: " + waitingForAcks);
                ex.printStackTrace();

                long cleanUpStartTime = System.currentTimeMillis();

                // Clean things up before aborting.
                // TODO: Move this to after we rollback so other reads/writes can proceed immediately without
                //       having to wait for us to clean-up.
                try {
                    cleanUpAfterConsistencyProtocol(true);
                } catch (Exception e) {
                    // We should still be able to continue, despite failing to clean up after ourselves...
                    LOG.error("Encountered error while cleaning up after the consistency protocol: ", e);
                }

                if (TransactionalRequestHandler.TX_EVENTS_ENABLED) {
                    long cleanUpEndTime = System.currentTimeMillis();
                    transactionAttempt.setConsistencyCleanUpTimes(cleanUpStartTime, cleanUpEndTime);
                }

                LOG.error("Exception encountered while waiting for ACKs: ", ex);
                exceptions.add(ex);
                canProceed = false;
                return;
            }
        }
        else {
            if (LOG.isDebugEnabled()) LOG.debug("We do not require any ACKs, so we can skip the rest of the consistency protocol.");
            needToUnsubscribe = false;
        }

        long cleanUpStartTime = System.currentTimeMillis();

        // Clean up ACKs, event operation, etc.
        try {
            // We always clean up when not using ZooKeeper. If we're using ZooKeeper, then we only clean up
            // if we issued invalidations, and that only happens when at least one ACK was required.
            if (!useZooKeeperForACKsAndINVs || totalNumberOfACKsRequired > 0)
                cleanUpAfterConsistencyProtocol(needToUnsubscribe);
        } catch (Exception e) {
            // We should still be able to continue, despite failing to clean up after ourselves...
            LOG.error("Encountered error while cleaning up after the consistency protocol: ", e);
        }

        if (TransactionalRequestHandler.TX_EVENTS_ENABLED) {
            long cleanUpEndTime = System.currentTimeMillis();
            transactionAttempt.setConsistencyCleanUpTimes(cleanUpStartTime, cleanUpEndTime);
        }

        // Steps 6 and 7 happen automatically. We can return from this function to perform the writes.
        this.canProceed = true;
    }

    /**
     * This is used as a listener for ZooKeeper events during the consistency protocol. This updates the
     * datastructures tracking the ACKs we're waiting on in response to follower NNs dropping out during the
     * consistency protocol.
     *
     * This function is called once AFTER being set as the event listener to ensure no membership changes occurred
     * between when the leader NN first checked group membership to create the ACK entries and when the leader begins
     * monitoring explicitly for changes in group membership.
     *
     * This is synchronized because it modified state and can be called by multiple threads (e.g., event listeners).
     *
     * @param deploymentNumber The deployment number of the given group. Note that the group name is just
     *                         "namenode" + deploymentNumber.
     * @param calledManually Indicates that we called this function manually rather than automatically in response
     *                       to a ZooKeeper event. Really just used for debugging.
     */
    private synchronized void checkAndProcessMembershipChanges(int deploymentNumber, boolean calledManually)
            throws Exception {
        Set<Long> deploymentAcks = waitingForAcksPerDeployment.get(deploymentNumber);
        if (deploymentAcks == null) {
            return;
        }

        String groupName = "namenode" + deploymentNumber;

        if (LOG.isDebugEnabled()) {
            if (calledManually) LOG.debug("ZooKeeper detected membership change for group: " + groupName);
            else LOG.debug("Checking for membership changes for deployment #" + deploymentNumber);
        }

        ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();

        long s = System.nanoTime();
        // Get the current members.
        List<String> groupMemberIdsAsStrings = zkClient.getPermanentGroupMembers(groupName);

        if (LOG.isTraceEnabled()) {
            long t = System.nanoTime();
            LOG.trace("Retrieved members of deployment " + deploymentNumber + " in " + ((t-s)/1.0e6) + " ms.");
        }

        // Convert from strings to longs.
        List<Long> groupMemberIds = groupMemberIdsAsStrings.stream()
                .mapToLong(Long::parseLong)
                .boxed()
                .collect(Collectors.toList());

        // For each NN that we're waiting on, check that it is still a member of the group. If it is not, then remove it.
        List<Long> removeMe = new ArrayList<>();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Deployment #" + deploymentNumber + " has " + groupMemberIds.size() +
                    " active instance(s): " + StringUtils.join(groupMemberIds, ", "));
            LOG.trace("ACKs required from deployment #" + deploymentNumber + ": " +
                    StringUtils.join(deploymentAcks, ", "));
        }

        // Compare the group member IDs to the ACKs from JUST this deployment, not the master list of all ACKs from all
        // deployments. If we were to iterate over the master list of all ACKs (that is not partitioned by deployment),
        // then any ACK from another deployment would obviously not be in the groupMemberIds variable, since those group
        // member IDs are just from one particular deployment.
        for (long memberId : deploymentAcks) {
            if (!groupMemberIds.contains(memberId))
                removeMe.add(memberId);
        }

        // Stop waiting on any NNs that have failed since the consistency protocol began.
        if (removeMe.size() > 0) {
            LOG.warn("Found " + removeMe.size()
                    + " NameNode(s) that we're waiting on, but are no longer active.");
            LOG.warn("IDs of these NameNodes: " + removeMe);
            removeMe.forEach(id -> {
                waitingForAcks.remove(id);   // Remove from the set of ACKs we're still waiting on.
                deploymentAcks.remove(id);   // Remove from the set of ACKs specific to the deployment.
                countDownLatch.countDown(); // Decrement the count-down latch once for each entry we remove.
            });
        }

        // If after removing all the failed follower NNs, we are not waiting on anybody, then we can just return.
        if (LOG.isTraceEnabled()) {
            if (removeMe.size() > 0 && waitingForAcks.size() == 0) {
                LOG.trace("After removal of " + removeMe.size() +
                        " failed follower NameNode(s), we have all required ACKs.");
            } else if (removeMe.size() > 0) {
                LOG.trace("After removal of " + removeMe.size() +
                        " failed follower NameNode(s), we are still waiting on " + waitingForAcks.size() +
                        " more ACK(s) from " + waitingForAcks + ".");
            } else if (waitingForAcks.size() > 0) {
                LOG.trace("No NNs removed from waiting-on ACK list. Still waiting on " + waitingForAcks.size() +
                        " more ACK(s) from " + waitingForAcks + ".");
            } else {
                LOG.trace("No NNs removed from waiting-on ACK list. Not waiting on any ACKs.");
            }
        }
    }

    /**
     * This function performs steps 4 and 5 of the consistency protocol. We, as the leader, simply have to wait for the
     * follower NNs to ACK our write operations.
     */
    private void waitForAcks() throws Exception {
        if (LOG.isDebugEnabled()) LOG.debug("=-----=-----= Steps 4 & 5 - Waiting for ACKs =-----=-----=");

        ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();
        int localDeploymentNumber = serverlessNameNodeInstance.getDeploymentNumber();

        // Start listening for changes in group membership. We've already added a listener for all deployments
        // that are not our own in the 'joinOtherDeploymentsAsGuest()' function, so this is just for our local deployment.
        zkClient.addListener(serverlessNameNodeInstance.getFunctionName(), watchedEvent -> {
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    checkAndProcessMembershipChanges(localDeploymentNumber, false);
                } catch (Exception e) {
                    LOG.error("Encountered error while reacting to ZooKeeper event.");
                    e.printStackTrace();
                }
            }
        });

        long s = System.currentTimeMillis();

        // This is a sanity check. For all non-local deployments, there is a small chance there was a membership changed
        // in-between us joining the group/creating ACKs and establishing listeners and all that, so this makes sure
        // our global image of deployment membership is correct. Likewise, there is a chance that membership for our local
        // deployment has changed in between when we created the ACK entries and when we added the ZK listener just now.
        for (int deploymentNumber : involvedDeployments)
            checkAndProcessMembershipChanges(deploymentNumber, true);

        long t = System.currentTimeMillis();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Called `checkAndProcessMembershipChanges()` for " + involvedDeployments.size() +
                    " deployment(s) in " + (t - s) + " ms.");
            LOG.trace("Waiting for the remaining " + waitingForAcks.size() +
                    " ACK(s) now. Will timeout after " + serverlessNameNodeInstance.getTxAckTimeout() + " milliseconds.");
            LOG.trace("Count value of CountDownLatch: " + countDownLatch.getCount());
        }

        s = System.currentTimeMillis();
        // Wait until we're done. If the latch is already at zero, then this will not block.
        boolean success;
        try {
            success = countDownLatch.await(serverlessNameNodeInstance.getTxAckTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            throw new IOException("Interrupted waiting for ACKs from other NameNodes. Waiting on a total of " +
                    waitingForAcks.size() + " ACK(s): " + StringUtils.join(waitingForAcks, ", "));
        }
        t = System.currentTimeMillis();

        if (LOG.isTraceEnabled()) LOG.trace("Spent " + (t - s) + " ms waiting on ACKs.");

        if (!success) {
            LOG.warn("Timed out while waiting for ACKs from other NNs. Waiting on a total of " +
                    waitingForAcks.size() + " ACK(s): " + StringUtils.join(waitingForAcks, ", "));
            LOG.warn("Checking liveliness of NNs that we're still waiting on...");

            // If we timed-out, verify that the NameNodes we're waiting on are still, in fact, alive.
            for (int deployment : involvedDeployments) {
                Set<Long> waitingOnInDeployment = waitingForAcksPerDeployment.get(deployment);

                if (waitingOnInDeployment == null)
                    continue;

                for (long nameNodeId : waitingOnInDeployment) {
                    boolean isAlive = zkClient.checkForPermanentGroupMember(deployment, Long.toString(nameNodeId));

                    if (!isAlive) {
                        LOG.warn("NN " + nameNodeId + " is no longer alive, yet we're still waiting on them.");
                        waitingForAcks.remove(nameNodeId);
                    } else {
                        LOG.error("NN " + nameNodeId + " is still alive, but has not ACK'd for some reason.");
                    }
                }
            }

            if (waitingForAcks.size() == 0) {
                LOG.warn("There are no unreceived ACKs after checking liveliness of other NNs.");
            } else {
                LOG.error("There are still unreceived ACKs after checking liveliness of other NNs.");
                throw new IOException("Timed out while waiting for ACKs from other NameNodes. Waiting on a total of " +
                        waitingForAcks.size() + " ACK(s): " + StringUtils.join(waitingForAcks, ", "));
            }
        }

        assert(waitingForAcks.isEmpty());
        if (LOG.isDebugEnabled()) LOG.debug("We have received all required ACKs for write operation " + operationId + ".");
    }

    /**
     * Perform any necessary clean-up steps after the consistency protocol has completed.
     * This includes unsubscribing from ACK table events, removing the ACK entries from the table in NDB, leaving any
     * deployments that we joined as a guest, etc.
     *
     * @param needToUnsubscribe If true, then we still need to unsubscribe from ACK events. If false, then we
     *                          already unsubscribed from ACK events (presumably because we found that we didn't
     *                          actually need any ACKs and just unsubscribed immediately).
     */
    private void cleanUpAfterConsistencyProtocol(boolean needToUnsubscribe)
            throws Exception {
        if (LOG.isDebugEnabled()) LOG.debug("Performing clean-up procedure for consistency protocol now.");
        //long s = System.currentTimeMillis();
        // Unsubscribe and unregister event listener if we haven't done so already. (If we were the only active NN in
        // our deployment at the beginning of the protocol, then we would have already unsubscribed by this point.)

        if (!useZooKeeperForACKsAndINVs) {
            if (needToUnsubscribe) {
                unsubscribeFromAckEvents();
            }

            deleteWriteAcknowledgements();
        }
        else {
            // Remove the ZNodes we created during the execution of the consistency protocol.
            for (int deployment : involvedDeployments) {
                serverlessNameNodeInstance.getZooKeeperClient().removeInvalidation(operationId, "namenode" + deployment);
            }
        }
    }

    /**
     * Delete write acknowledgement entries we created during the consistency protocol.
     */
    private void deleteWriteAcknowledgements() {
        for (Map.Entry<Integer, List<WriteAcknowledgement>> entry : writeAcknowledgementsMap.entrySet())
            serverlessNameNodeInstance.enqueueAcksForDeletion(entry.getValue());
    }

    /**
     * Leave deployments that we joined as a guest.
     */
    private void leaveDeployments() throws Exception {
        ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();

        String memberId = serverlessNameNodeInstance.getId() + "-" + Thread.currentThread().getId();
        for (int deployentNumber : involvedDeployments) {
            if (deployentNumber == serverlessNameNodeInstance.getDeploymentNumber())
                continue;

            zkClient.leaveGroup("namenode" + deployentNumber, memberId, false);
            zkClient.removeListener("namenode" + deployentNumber, this.watchers.get(deployentNumber));
        }
    }

    /**
     * Unregister ourselves as an event listener for ACK table events.
     */
    private void unsubscribeFromAckEvents()
            throws StorageException {
        if (LOG.isDebugEnabled()) LOG.debug("Unsubscribing from ACK events now.");
        for (int deploymentNumber : involvedDeployments) {
            String eventName = HopsEvent.ACK_EVENT_NAME_BASE + deploymentNumber;
            EventManager eventManager = serverlessNameNodeInstance.getNdbEventManager();

            // This returns a semaphore that we could use to wait, but we don't really care when the operation gets dropped.
            // We just don't care to receive events anymore. We can continue just fine if we receive them for a bit.
            eventManager.requestDropSubscription(eventName, this);
        }
    }

    /**
     * Used as the event handler for NodeCreated events from ZooKeeper.
     * This is what gets executed when we receive an ACK.
     *
     * This method is synchronized because we modify sets from the 'waitingForAcksPerDeployment' map, which
     * is accessed in other places.
     *
     * @param path The path of the ACK that we received.
     */
    private synchronized void handleZooKeeperEvent(String path) {
        if (LOG.isTraceEnabled()) LOG.trace("Received ZooKeeper 'NodeCreated' event with path '" + path + "'");

        String[] tokens = path.split("/");
        long followerId = Long.parseLong(tokens[tokens.length - 1]);
        if (!waitingForAcks.contains(followerId))
            return;

        int mappedDeployment = nameNodeIdToDeploymentNumberMapping.get(followerId);
        if (LOG.isDebugEnabled()) LOG.debug("Received ACK from NameNode " + followerId + " (deployment = " +
                mappedDeployment + ")!");

        waitingForAcks.remove(followerId);

        Set<Long> deploymentAcks = waitingForAcksPerDeployment.get(mappedDeployment);
        deploymentAcks.remove(followerId);

        countDownLatch.countDown();
    }

    // Made this synchronized since it modified sets from the 'waitingForAcksPerDeployment' map,
    // which can be modified by multiple threads via event handlers.
    @Override
    public synchronized void eventReceived(HopsEventOperation eventData, String eventName) {
        if (!eventName.contains(HopsEvent.ACK_EVENT_NAME_BASE)) {
            LOG.error("HopsTransactionalRequestHandler received unexpected event " + eventName + "!");
            return;
        }

        String eventType = eventData.getEventType();
        if (eventType.equals(HopsEventType.INSERT)) // We don't care about INSERT events.
            return;

        // First, verify that this event pertains to our write operation. If it doesn't, we just return.
        long writeOpId = eventData.getLongPostValue(TablesDef.WriteAcknowledgementsTableDef.OPERATION_ID);
        long nameNodeId = eventData.getLongPostValue(TablesDef.WriteAcknowledgementsTableDef.NAME_NODE_ID);
        boolean acknowledged = eventData.getBooleanPostValue(TablesDef.WriteAcknowledgementsTableDef.ACKNOWLEDGED);
        int mappedDeployment = nameNodeIdToDeploymentNumberMapping.get(nameNodeId);

        if (writeOpId != operationId) // If it is for a different write operation, then we don't care about it.
            return;

        if (acknowledged) {
            // It's possible that there are multiple transactions going on simultaneously, so we may receive ACKs for
            // NameNodes that we aren't waiting on. We just ignore these.
            // TODO: May want to verify, in general, that we aren't actually receiving too many ACKs, like routinely
            //       verify that there isn't a bug causing NameNodes to ACK the same entry several times.
            if (!waitingForAcks.contains(nameNodeId))
                return;

            if (LOG.isDebugEnabled()) LOG.debug("Received ACK from NameNode " + nameNodeId + " (deployment = " +
                    mappedDeployment + ")!");

            waitingForAcks.remove(nameNodeId);

            Set<Long> deploymentAcks = waitingForAcksPerDeployment.get(mappedDeployment);
            deploymentAcks.remove(nameNodeId);

            countDownLatch.countDown();
        }
    }

    /**
     * Return the table name to subscribe to for ACK events, given the deployment number.
     * @param deploymentNumber Deployment number for which a subscription should be created for the associated table.
     * @return The name of the table for which an event subscription should be created.
     * @throws StorageException If the deployment number refers to a non-existent deployment.
     */
    private String getTargetTableName(int deploymentNumber) throws StorageException {
        switch (deploymentNumber) {
            case 0:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME0;
            case 1:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME1;
            case 2:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME2;
            case 3:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME3;
            case 4:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME4;
            case 5:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME5;
            case 6:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME6;
            case 7:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME7;
            case 8:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME8;
            case 9:
                return TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME9;
            default:
                throw new StorageException("Unsupported deployment number: " + deploymentNumber);
        }
    }

    /**
     * Perform Step (1) of the consistency protocol:
     *    The Leader NN begins listening for changes in group membership from ZooKeeper.
     *    The Leader will also subscribe to events on the ACKs table for reasons that will be made clear shortly.
     */
    private void subscribeToAckEvents() throws StorageException, InterruptedException {
        if (LOG.isDebugEnabled()) LOG.debug("=-----=-----= Step 1 - Subscribing to ACK Events =-----=-----=");

        // Each time we request that the event manager create an event subscription for us, it returns a semaphore
        // we can use to block until the event operation is created. We want to do this here, as we do not want
        // to continue until we know we'll receive the event notifications.
        List<EventRequestSignaler> eventRequestSignalers = new ArrayList<>();

        EventManager eventManager = serverlessNameNodeInstance.getNdbEventManager();
        for (int deploymentNumber : involvedDeployments) {
            String targetTableName = getTargetTableName(deploymentNumber);
            String eventName = HopsEvent.ACK_EVENT_NAME_BASE + deploymentNumber;

            ExponentialBackOff backOff = new ExponentialBackOff.Builder()
                    .setInitialIntervalMillis(100)
                    .setMaximumIntervalMillis(5000)
                    .setMaximumRetries(99)
                    .build();

            boolean success = false;
            long sleepInterval;
            while ((sleepInterval = backOff.getBackOffInMillis()) != 0) {
                try {
                    EventRequestSignaler eventRequestSignaler = eventManager.requestCreateEvent(eventName, targetTableName,
                            eventManager.getAckTableEventColumns(), false, true,this,
                            eventManager.getAckEventTypeIDs());
                    eventRequestSignalers.add(eventRequestSignaler);
                    success = true;
                    break;
                } catch (StorageException ex) {
                    com.esotericsoftware.minlog.Log.error("Encountered StorageException while requesting event/subscription creation for event '" +
                            eventName + "': " + ex);
                    Thread.sleep(sleepInterval);
                }
            }

            if (!success) {
                throw new IllegalStateException("Failed to create event/subscription for event '" + eventName + "' after " +
                        backOff.getNumberOfRetries() + " attempts.");
            }
        }

        if (LOG.isDebugEnabled()) LOG.debug("Acquiring " + eventRequestSignalers.size() + " semaphore(s) now.");

        for (EventRequestSignaler eventRequestSignaler : eventRequestSignalers) {
            eventRequestSignaler.acquire();
        }

        if (LOG.isDebugEnabled()) LOG.debug("Successfully acquired " + eventRequestSignalers.size() + " semaphore(s).");
    }

    /**
     * Perform Step (1) of the consistency protocol:
     *    Create the ACK instances that we will add to NDB. We do NOT add them in this function; we merely compute
     *    them (i.e., create the objects). In some cases, we will find that we don't create any, in which case we
     *    skip subscribing to the ACK events table. If we create at least one ACK object in this method however, we
     *    will first subscribe to ACK events, then we will add the ACKs to intermediate storage.
     *
     * IMPORTANT (ZooKeeper):
     * The full extent of this function is only performed when using NDB as the intermediate storage medium for the
     * consistency protocol. If we're using ZooKeeper, then all we do is determine the number of ACKs required. We
     * do not actually compute the ACK records here, as we do things a little differently when using ZooKeeper.
     *
     * We do not create one ACK for each active NN instance. Instead, we issue a single invalidation by creating one
     * ZNode directory. The data on that ZNode contains all necessary information (e.g., the INodes being invalidated).
     * The active NNs simply create ephemeral ZNodes underneath that directory, which serve as the ACKs.
     *
     * @param transactionStartTime The UTC timestamp at which this write operation began.
     *
     * @return The number of ACK records that we added to intermediate storage.
     */
    private int computeAckRecords(long transactionStartTime)
            throws Exception {
        if (LOG.isDebugEnabled()) LOG.debug("=-----=-----= Step 0 - Pre-Compute NDB ACK Records In-Memory =-----=-----=");
        ZKClient zkClient = serverlessNameNodeInstance.getZooKeeperClient();
        assert(zkClient != null);

        // Sum the number of ACKs required per deployment. We use this value when creating the
        // CountDownLatch that blocks us from continuing with the protocol until all ACKs are received.
        int totalNumberOfACKsRequired = 0;

        // If there are no active instances in the deployments that are theoretically involved, then we just remove
        // them from the set of active deployments, as we don't need ACKs from them, nor do we need to store any INVs.
        Set<Integer> toRemove = new HashSet<>();

        writeAcknowledgementsMap = new HashMap<>();

        // For each deployment (which at least includes our own), get the current members and register a membership-
        // changed listener. This enables us to monitor for any changes in group membership; in particular, we will
        // receive notifications if any NameNodes leave a deployment.
        for (int deploymentNumber : involvedDeployments) {
            List<WriteAcknowledgement> writeAcknowledgements = new ArrayList<>();

            long s = System.nanoTime();
            List<String> groupMemberIds = zkClient.getPermanentGroupMembers("namenode" + deploymentNumber);
            if (LOG.isTraceEnabled())
                LOG.trace("Retrieved active NNs in deployment " + deploymentNumber + " in " +
                        ((System.nanoTime() - s) / 1.0e6) + " ms.");

            if (groupMemberIds.size() == 0) {
                toRemove.add(deploymentNumber);
                continue;
            } else if (LOG.isDebugEnabled()) {
                if (groupMemberIds.size() == 1)
                    LOG.trace("There is 1 active instance in deployment #" + deploymentNumber + " at the start of consistency protocol: " + groupMemberIds.get(0) + ".");
                else
                    LOG.trace("There are " + groupMemberIds.size() + " active instances in deployment #" + deploymentNumber + " at the start of consistency protocol: " + StringUtils.join(groupMemberIds, ", "));
            }

            Set<Long> acksForCurrentDeployment = waitingForAcksPerDeployment.computeIfAbsent(deploymentNumber, depNum -> new HashSet<Long>());

            // Iterate over all the current group members. For each group member, we create a WriteAcknowledgement object,
            // which we'll persist to intermediate storage. We skip ourselves, as we do not need to ACK our own write. We also
            // create an entry for each follower NN in the `writeAckMap` to keep track of whether they've ACK'd their entry.
            for (String memberIdAsString : groupMemberIds) {
                long memberId = Long.parseLong(memberIdAsString);
                nameNodeIdToDeploymentNumberMapping.put(memberId, deploymentNumber); // Note which deployment this NN is from.

                // We do not need to add an entry for ourselves. We have to check everytime rather than just once
                // at the end (like we do when using ZooKeeper) as we do not want to waste time creating ACK
                // record for us, seeing as we do not need an ACK from ourselves.
                if (memberId == serverlessNameNodeInstance.getId())
                    continue;

                // Master list of all the NNs we need ACKs from.
                waitingForAcks.add(memberId);

                // We're iterating over each deployment in the outer loop.
                // This is the list of NNs from which we need an ACK for the current deployment.
                acksForCurrentDeployment.add(memberId);

                if (!useZooKeeperForACKsAndINVs)
                    // These are just all the WriteAcknowledgement objects that we're going to store in the database.
                    writeAcknowledgements.add(new WriteAcknowledgement(memberId, deploymentNumber, operationId, false, transactionStartTime, serverlessNameNodeInstance.getId()));

                totalNumberOfACKsRequired += 1;
            }

            // Creating the mapping from the current deployment (we're iterating over all deployments right now)
            // to the set of write acknowledgements to be stored in intermediate storage for that specific deployment.
            // (Each deployment has its own ACK table in NDB.)
            if (!useZooKeeperForACKsAndINVs)
                writeAcknowledgementsMap.put(deploymentNumber, writeAcknowledgements);
        }

        if (toRemove.size() > 0)
            if (LOG.isDebugEnabled()) LOG.debug("Removing the following deployments as they contain zero active instances: " +
                    StringUtils.join(toRemove, ", "));
        involvedDeployments.removeAll(toRemove);
        if (LOG.isDebugEnabled()) LOG.debug("Grand total of " + totalNumberOfACKsRequired + " ACKs required.");

        // Instantiate the CountDownLatch variable. The value is set to the number of ACKs that we need
        // before we can proceed with the transaction. Receiving an ACK and a follower NN leaving the group
        // will trigger a decrement.
        countDownLatch = new CountDownLatch(totalNumberOfACKsRequired);

        // This will be zero if we are the only active NameNode.
        return totalNumberOfACKsRequired;
    }

    private void issueInvalidationsZooKeeper(Collection<INode> invalidatedINodes) throws Exception {
        if (LOG.isDebugEnabled()) LOG.debug("=-----=-----= Step 3 - Issuing Initial Invalidations via ZooKeeper =-----=-----=");

        List<Long> invalidatedINodeIDs;

        // Will be null for subtree operations?
        if (invalidatedINodes != null)
            invalidatedINodeIDs = invalidatedINodes.stream().map(INode::getId).collect(Collectors.toList());
        else
            invalidatedINodeIDs = new ArrayList<>();

        ZooKeeperInvalidation invalidation = new ZooKeeperInvalidation(serverlessNameNodeInstance.getId(),
                operationId, invalidatedINodeIDs, subtreeOperation, subtreeRoot);

        if (LOG.isDebugEnabled()) LOG.debug("Issuing invalidation " + invalidation + " for " + involvedDeployments.size() + " deployment(s).");
        for (int deployment : involvedDeployments) {
            if (LOG.isTraceEnabled()) LOG.trace("Issuing ZooKeeper invalidation for deployment " + deployment + ".");
            serverlessNameNodeInstance.getZooKeeperClient().putInvalidation(
                    invalidation, "namenode" + deployment, watchedEvent -> {
                        if (watchedEvent.getType() == Watcher.Event.EventType.NodeCreated)
                            handleZooKeeperEvent(watchedEvent.getPath());
                    });
        }
    }

    /**
     * Perform Step (3) of the consistency protocol:
     *    The leader sets the INV flag of the target INode to 1 (i.e., true), thereby triggering a round of
     *    INVs from intermediate storage (NDB).
     *
     * @param invalidatedINodes The INodes involved in this write operation. We must invalidate these INodes.
     * @param transactionStartTime The time at which the transaction began.
     */
    private void issueInvalidationsNDB(Collection<INode> invalidatedINodes, long transactionStartTime)
            throws StorageException {
        if (LOG.isDebugEnabled()) LOG.debug("=-----=-----= Step 3 - Issuing Initial Invalidations via NDB =-----=-----=");

        InvalidationDataAccess<Invalidation> dataAccess =
                (InvalidationDataAccess<Invalidation>) HdfsStorageFactory.getDataAccess(InvalidationDataAccess.class);

        Map<Integer, List<Invalidation>> invalidationsMap = new HashMap<>();

        for (INode invalidatedINode : invalidatedINodes) {
            int mappedDeploymentNumber = serverlessNameNodeInstance.getMappedDeploymentNumber(invalidatedINode);
            List<Invalidation> invalidations = invalidationsMap.getOrDefault(mappedDeploymentNumber, null);

            if (invalidations == null) {
                invalidations = new ArrayList<>();
                invalidationsMap.put(mappedDeploymentNumber, invalidations);
            }

            // int inodeId, int parentId, long leaderNameNodeId, long transactionStartTime, long operationId
            invalidations.add(new Invalidation(invalidatedINode.getId(), invalidatedINode.getParentId(),
                    serverlessNameNodeInstance.getId(), transactionStartTime, operationId));
        }

        for (Map.Entry<Integer, List<Invalidation>> entry : invalidationsMap.entrySet()) {
            int deploymentNumber = entry.getKey();
            List<Invalidation> invalidations = entry.getValue();

            int numInvalidations = invalidations.size();
            if (numInvalidations == 0) {
                if (LOG.isDebugEnabled()) LOG.debug("Adding 0 INV entries for deployment #" + deploymentNumber + ".");
                continue;
            }
            else if (numInvalidations == 1)
                if (LOG.isDebugEnabled()) LOG.debug("Adding 1 INV entry for deployment #" + deploymentNumber + ".");
            else
                if (LOG.isDebugEnabled()) LOG.debug("Adding " + numInvalidations + " INV entries for deployment #" +
                        deploymentNumber + ".");

            dataAccess.addInvalidations(invalidations, deploymentNumber);
        }
    }    
}
