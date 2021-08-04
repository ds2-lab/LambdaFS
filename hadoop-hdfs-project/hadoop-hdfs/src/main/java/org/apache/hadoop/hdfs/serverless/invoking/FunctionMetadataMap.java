package org.apache.hadoop.hdfs.serverless.invoking;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.ArgumentContainer;

import java.util.HashMap;

/**
 * Maintains a cache that maps files to the particular serverless functions which cache that
 * file's (or directory's) metadata.
 *
 * Objects of this class are utilized by clients of Serverless HopsFS.
 */
public class FunctionMetadataMap {
    private static final Log LOG = LogFactory.getLog(FunctionMetadataMap.class);

    private final HashMap<String, String> cache;

    public FunctionMetadataMap() {
        cache = new HashMap<String, String>();
    }

    /**
     * Check if the cache contains an entry for the particular file or directory.
     * @param path The path of the file or directory of interest.
     * @return `true` if the cache contains an entry for the given file or directory, otherwise `false`.
     */
    public boolean containsEntry(String path) {
        return cache.containsKey(path);
    }

    /**
     * Return the particular serverless functions responsible for caching the metadata for the given file or directory.
     * @return the name of the associated serverless function, or null if no
     *         entry exists in the map for this function yet.
     */
    public String getFunction(String file) {
        if (cache.containsKey(file)) {
            String functionName = cache.get(file);
            LOG.debug(String.format("Returning function %s for file %s", file, functionName));
            return functionName;
        }
        else {
            LOG.debug("No entry associated with file " + file);
            return null;
        }
    }

    /**
     * Add an entry to the cache. Will not overwrite an existing entry unless parameter `overwriteExisting` is true.
     * @param path The file or directory (i.e., key) for which we are adding an entry to the cache.
     * @param function The serverless function (i.e., value) associated with the given file.
     * @param overwriteExisting Overwrite an existing entry.
     * @return `true` if entry was added to the cache, otherwise `false`.
     */
    public boolean addEntry(String path, String function, boolean overwriteExisting) {
        // Only add the file to the cache if we're supposed to overwrite existing entries or
        // if there does not already exist an entry for the given file/directory.
        if (overwriteExisting || !cache.containsKey(path)) {
            cache.put(path, function);
            return true;
        }
        return false;
    }

    /**
     * Return the number of entries in the cache (i.e., the size of the cache).
     */
    public int size() {
        return cache.size();
    }
}
