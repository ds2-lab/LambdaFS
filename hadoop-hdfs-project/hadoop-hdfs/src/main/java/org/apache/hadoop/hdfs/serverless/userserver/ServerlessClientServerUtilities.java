package org.apache.hadoop.hdfs.serverless.userserver;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import io.hops.exception.TransientDeadLockException;
import io.hops.exception.TransientStorageException;
import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.serverless.exceptions.NameNodeException;
import org.apache.hadoop.hdfs.serverless.exceptions.TcpRequestCancelledException;
import org.apache.hadoop.hdfs.serverless.consistency.ActiveServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.consistency.ActiveServerlessNameNodeList;
import org.apache.hadoop.hdfs.serverless.exceptions.UnsupportedObjectPayloadException;
import org.apache.hadoop.hdfs.serverless.execution.results.*;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;

/**
 * Utility functions exposed by both TCP clients and servers.
 */
public class ServerlessClientServerUtilities {
    private static final Log LOG = LogFactory.getLog(ServerlessClientServerUtilities.class);

    /**
     * This operation is used when a NameNode is first connecting to and registering with a client.
     */
    public static final String OPERATION_REGISTER = "REGISTER";

    /**
     * This operation is used when a NameNode is returning the result of some FS operation back to a client.
     */
    public static final String OPERATION_RESULT = "RESULT";

    /**
     * This operation is used when the NameNode just wants to report some information to the client.
     *
     * As of right now, this information will just be logged/use for debugging purposes.
     */
    public static final String OPERATION_INFO = "INFO";

    /**
     * Register all the classes that are going to be sent over the network.
     *
     * This must be done on both the client and the server before any network communication occurs.
     * The exact same classes are to be registered in the exact same order.
     * @param kryo The Kryo object obtained from a given Kryo TCP client/server via getKryo().
     */
    public static synchronized void registerClassesToBeTransferred(Kryo kryo) {
        kryo.setReferences(true);
        kryo.setRegistrationRequired(true);

        UnmodifiableCollectionsSerializer.registerSerializers( kryo );

        //////////////////////////////////////////////////////////
        // MAKE SURE THESE CLASSES HAVE A DEFAULT CONSTRUCTOR.  //
        // IF THEY DON'T, THEN SENDING THEM OVER TCP WILL       //
        // PROBABLY RESULT IN THE CONNECTION BEING TERMINATED   //
        // WITHOUT ANY SORT OF INDICATION AS TO WHY IT GOT      //
        // TERMINATED. VERY ANNOYING.                           //
        //////////////////////////////////////////////////////////

        kryo.register(NameNodeResult.class);
        kryo.register(TcpUdpRequestPayload.class);
        kryo.register(NameNodeResultWithMetrics.class);
        kryo.register(TransactionEvent.class);
        kryo.register(TransactionAttempt.class);
        kryo.register(LocatedBlocks.class);
        kryo.register(java.lang.Exception.class, new JavaSerializer());
        kryo.register(LocatedBlock.class);
        kryo.register(NameNodeException.class, new JavaSerializer());
        kryo.register(UnsupportedObjectPayloadException.class, new JavaSerializer());
        kryo.register(Token.class);
        kryo.register(TokenRenewer.class);
        kryo.register(BlockTokenIdentifier.class);
        kryo.register(BlockTokenIdentifier.AccessMode.class);
        kryo.register(EnumSet.class);
        kryo.register(Block.class);
        kryo.register(DatanodeInfoWithStorage.class);
        kryo.register(DatanodeInfoWithStorage[].class);
        kryo.register(StorageType.class);
        kryo.register(ExtendedBlock.class);
        kryo.register(NamespaceInfo.class);
        kryo.register(LastBlockWithStatus.class);
        kryo.register(IOException.class, new JavaSerializer());
        kryo.register(HdfsFileStatus.class);
        kryo.register(HdfsFileStatus[].class);
        kryo.register(java.lang.Integer[].class);
        kryo.register(DirectoryListing.class);
        kryo.register(StackTraceElement.class);
        kryo.register(StackTraceElement[].class);
        kryo.register(java.util.Collections. class);
        kryo.register(Boolean.class);
        kryo.register(FsServerDefaults.class);
        kryo.register(ActiveServerlessNameNodeList.class);
        kryo.register(ActiveServerlessNameNode.class);
        kryo.register(Throwable.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(byte[].class);
        kryo.register(FsPermission.class);
        kryo.register(FileEncryptionInfo.class);
        kryo.register(CryptoProtocolVersion.class);
        kryo.register(CipherSuite.class);
        kryo.register(FsAction.class);
        kryo.register(TcpRequestCancelledException.class, new JavaSerializer());
        kryo.register(java.lang.ClassCastException.class, new JavaSerializer());
        kryo.register(NullResult.class);
        kryo.register(DuplicateRequest.class);
        kryo.register(FileNotFoundException.class, new JavaSerializer());
        kryo.register(NullPointerException.class, new JavaSerializer());
        kryo.register(TransientDeadLockException.class, new JavaSerializer());
        kryo.register(TransientStorageException.class, new JavaSerializer());
        kryo.register(IllegalArgumentException.class, new JavaSerializer());
        kryo.register(org.apache.hadoop.fs.FileAlreadyExistsException.class, new JavaSerializer());
        kryo.register(Collections.EMPTY_LIST.getClass());
    }
}
