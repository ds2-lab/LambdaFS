package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides utility methods that may be used by the serverless name node during routine operation, but particularly
 * when interfacing with the serverless API (e.g., when extracting arguments from an invocation payload).
 */
public class ServerlessUtilities {

    /**
     * Extract all the arguments for this function and return them in a HashMap.
     *
     * TODO: Finish this function. It currently does nothing.
     *
     * @param arguments The arguments encoded as JSON as passed to the serverless function in the invocation payload.
     * @param method java.reflect.Method object of the method for which we are extracting arguments.
     * @param parameterNames The names of the parameters. These must be in the exact same order as they appear in the
     *                       method's definition in order for this function to work properly. This function will NOT
     *                       verify that the order is correct for you.
     *
     *                       The key corresponding to a particular parameter should be identical to its name. So if
     *                       we have a parameter "foo", there should be a key "foo" in `arguments` whose value is the
     *                       value of the "foo" parameter.
     */
    public static Object[] extractArguments(
            JsonObject arguments,
            Method method,
            String[] parameterNames) {
        // First, get a mapping from each parameter's name to its type.
        HashMap<String, Class<?>> argumentNameTypeMapping = getArgumentNameToTypeMapping(method, parameterNames);

        Object[] actualParameters = new Object[argumentNameTypeMapping.size()];

        int index = 0;
        for (Map.Entry<String, Class<?>> entry : argumentNameTypeMapping.entrySet()) {
            String argName = entry.getKey();
            Class<?> clazz = entry.getValue();

            // TODO: Deserialize arguments according to their type...

            index++;
        }

        throw new NotImplementedException("This function has not yet been implemented.");
    }

    /**
     * Given a path to a file or directory "X", extract the path to the parent directory of "X".
     *
     * @param originalPath Path to file or directory whose parent's path is desired.
     *
     * @return The path to the parent directory of {@code originalPath}.
     */
    public static String extractParentPath(String originalPath) {
        // First, we get the parent of whatever file or directory is passed in, as we cache by parent directory.
        // Thus, if we received mapping info for /foo/bar, then we really have mapping info for anything of the form
        // /foo/*, where * is a file or terminal directory (e.g., "bar" or "bar/").
        Path parentPath = Paths.get(originalPath).getParent();
        String pathToCache = null;

        // If there is no parent, then we are caching metadata mapping information for the root.
        if (parentPath == null) {
            // assert(originalPath.equals("/") || originalPath.isEmpty());
            pathToCache = originalPath;
        } else {
            pathToCache = parentPath.toString();
        }

        return pathToCache;
    }

    /**
     * Given a method and a list of parameter names for that method, return a mapping from each parameter's name
     * to its type.
     *
     * @param method java.reflect.Method object of the method in question.
     * @param parameterNames The names of the parameters. These must be in the exact same order as they appear in the
     *                       method's definition in order for this function to work properly. This function will NOT
     *                       verify that the order is correct for you.
     * @return A mapping from argument name (String) to argument type (Class).
     */
    public static HashMap<String, Class<?>> getArgumentNameToTypeMapping(Method method, String[] parameterNames) {
        Class<?>[] parameterTypes = method.getParameterTypes();

        HashMap<String, Class<?>> argumentNameTypeMapping = new HashMap<>();

        if (parameterNames.length != parameterTypes.length) {
            throw new IllegalArgumentException("The number of parameter names provided does not match the " +
                    "number of parameter types returned by `method.getParameterTypes()`");
        }

        for (int i = 0; i < parameterNames.length; i++) {
            String parameterName = parameterNames[i];
            Class<?> clazz = parameterTypes[i];

            argumentNameTypeMapping.put(parameterName, clazz);
        }

        return argumentNameTypeMapping;
    }

    /**
     * Source: https://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
     * Used to convert the activation ID of this serverless function to a long to use as the NameNode ID.
     *
     * In theory, this is done only when the function is cold.
     */
    public static long hash(String string) {
        long h = 1125899906842597L; // prime
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = 31*h + string.charAt(i);
        }
        return h;
    }

    /**
     * Deserialize an array of base64-encoded passed via Json as an argument to a file system operation.
     * @param key The key that the argument is stored in the fsArgs object.
     * @param fsArgs The file-system arguments passed by the client.
     * @param <T> The type of the object we're extracting from the Json array.
     * @return An array of T objects extracted from the Json arguments passed by the client.
     */
    public static <T> T[] deserializeArgumentArray(String key, JsonObject fsArgs) throws IOException, ClassNotFoundException {
        T[] array = null;
        if (fsArgs.has(key)) {
            // Decode and deserialize the DatanodeInfo[].
            JsonArray jsonArray = fsArgs.getAsJsonArray(key);
            array = (T[]) new Object[jsonArray.size()];

            for (int i = 0; i < jsonArray.size(); i++) {
                String base64 = jsonArray.get(i).getAsString();
                T newNode = (T) InvokerUtilities.base64StringToObject(base64);
                array[i] = newNode;
            }
        }

        return array;
    }

    /**
     * Deserialize an array of Strings passed via Json as an argument to a file system operation.
     *
     * @param key The key that the argument is stored in the fsArgs object.
     * @param fsArgs The file-system arguments passed by the client.
     * @return An array of Strings extracted from the Json arguments passed by the client.
     */
    public static String[] deserializeStringArray(String key, JsonObject fsArgs) {
        String[] array = null;
        if (fsArgs.has(key)) {
            // Decode and deserialize the DatanodeInfo[].
            JsonArray jsonArray = fsArgs.getAsJsonArray(key);
            array = new String[jsonArray.size()];

            for (int i = 0; i < jsonArray.size(); i++) {
                array[i] = jsonArray.get(i).getAsString();
            }
        }

        return array;
    }
}
