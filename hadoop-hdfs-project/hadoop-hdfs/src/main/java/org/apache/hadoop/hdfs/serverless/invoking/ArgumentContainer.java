package org.apache.hadoop.hdfs.serverless.invoking;

import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.serverless.tcpserver.TcpRequestPayload;
import org.apache.kerby.util.Base64;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Serves as a wrapper for passing arguments to serverless functions. This class simplifies the process of passing
 * arguments, as it handles the logic of packaging up arguments based on their type. That is, the logic for including
 * a particular object in the invocation payload for a NameNode varies depending on the type of the object. Rather
 * than place the burden of packaging up the arguments on the client (i.e., the entity invoking the NameNode), this
 * class handles it.
 *
 * This class essentially encapsulates the argument-packing logic, which ultimately makes the code cleaner and easier
 * to use.
 */
public class ArgumentContainer {
    private static final Log LOG = LogFactory.getLog(ArgumentContainer.class);

    /**
     * Contains primitive arguments, including Strings.
     */
    private final HashMap<String, Object> primitiveArguments;

    /**
     * Contains all byte[] arguments.
     */
    private final HashMap<String, byte[]> byteArrayArguments;

    /**
     * Contains all non-byte array arguments.
     */
    private final HashMap<String, Object[]> nonByteArrayArguments;

    /**
     * Contains all serializable object arguments.
     */
    private final HashMap<String, Serializable> objectArguments;

    /**
     * We maintain this HashMap so that we can just include it directly in the {@link TcpRequestPayload} object,
     * rather than having to build it up at the end.
     */
    private final HashMap<String, Object> allArguments;

    /**
     * Constructor. Initializes the internal fields used to keep track of arguments prior to packaging them up into
     * a format usable in the invocation request.
     */
    public ArgumentContainer() {
        primitiveArguments = new HashMap<>();
        byteArrayArguments = new HashMap<>();
        nonByteArrayArguments = new HashMap<>();
        objectArguments = new HashMap<>();

        allArguments = new HashMap<>();
    }

    public HashMap<String, Object> getAllArguments() { return this.allArguments; }

    /**
     * Return the parameter associated with the given key, or null if no such parameter exists.
     */
    public Object get(String key) {
        if (primitiveArguments.containsKey(key))
            return primitiveArguments.get(key);
        else if (objectArguments.containsKey(key))
            return objectArguments.get(key);
        else if (nonByteArrayArguments.containsKey(key))
            return nonByteArrayArguments.containsKey(key);

        return byteArrayArguments.getOrDefault(key, null);
    }

    /**
     * Return True if the Argument Container has a value for this key.
     */
    public boolean has(String key) {
        return primitiveArguments.containsKey(key) ||
                objectArguments.containsKey(key) ||
                nonByteArrayArguments.containsKey(key) ||
                byteArrayArguments.containsKey(key);
    }

    /**
     * Generic catch-all function for adding an argument.
     * @param key The name of the argument.
     * @param value The argument itself.
     */
    public void put(String key, Object value) {
        if (value == null) {
            return;
        }

        Class<?> clazz = value.getClass();

        // Check if `value` is a primitive.
        if (Number.class.isAssignableFrom(clazz) || Boolean.class.isAssignableFrom(clazz) || value instanceof String ||
                Character.class.isAssignableFrom(clazz))
            addPrimitive(key, value);
        // Check if `value` is an array. If it is, first check if it is byte[].
        else if (value.getClass().isArray()) {
            Class<?> componentClazz = value.getClass().getComponentType();

            if (byte.class.isAssignableFrom(componentClazz))
                addByteArray(key, (byte[])value);
            else
                addNonByteArray(key, (Object[])value);
        }
        // Check if `value` is an instance of Serializable.
        else if (value instanceof Serializable)
            addObject(key, (Serializable) value);
        else
            throw new IllegalArgumentException("Unsupported type for `value` argument: "
                    + value.getClass().getSimpleName());
    }

    /**
     * Add the given byte array to the arguments to be passed to the FS operation function.
     * @param key The name of the argument.
     * @param value The argument itself.
     */
    public void addByteArray(String key, byte[] value) {
        byteArrayArguments.put(key, value);
        allArguments.put(key, value);
    }

    /**
     * Add the given array (which must not be byte[] or Byte[]) to the collection of arguments.
     *
     * @param key The name of the argument.
     * @param value The argument itself.
     * @param <T> The type of the array being added.
     */
    public <T> void addNonByteArray(String key, T[] value) {
        if (value.getClass().getComponentType().isAssignableFrom(Byte.class)) {
            throw new IllegalArgumentException("Argument `value` must not be an array of byte/Byte.");
        }

        nonByteArrayArguments.put(key, value);
        allArguments.put(key, value);
    }

    /**
     * Add a primitive object to the argument list. Value should be an integer, float, double, boolean,
     * or character. Value can also be a String.
     *
     * @param key The name of the argument.
     * @param value The argument itself.
     * @param <T> The type of the value being added.
     */
    public <T> void addPrimitive(String key, T value) {
        assert(value.getClass().isPrimitive() || value instanceof String);
        primitiveArguments.put(key, value);
        allArguments.put(key, value);
    }

    /**
     * Add a Serializable object argument.
     * @param key The name of the argument.
     * @param value The argument itself.
     */
    public void addObject(String key, Serializable value) {
        // We do not want to Base64-encode a String, so instead we treat it like it is a primitive.
        // This works fine since Json can handle Strings.
        if (value instanceof String) {
            addPrimitive(key, value);
            return;
        }

        objectArguments.put(key, value);
        allArguments.put(key, value);
    }

    /**
     * Package the arguments into a JsonObject.
     * @return JsonObject containing all the arguments.
     */
    public JsonObject convertToJsonObject() throws IOException {
        JsonObject arguments = new JsonObject();

        for (Map.Entry<String, Object> entry : primitiveArguments.entrySet())
            packagePrimitive(entry.getKey(), entry.getValue(), arguments);

        for (Map.Entry<String, Serializable> entry : objectArguments.entrySet()) {
            String base64Encoded = InvokerUtilities.serializableToBase64String((Serializable)entry.getValue());
            arguments.addProperty(entry.getKey(), base64Encoded);
        }

        for (Map.Entry<String, byte[]> entry : byteArrayArguments.entrySet()) {
            String base64Encoded = Base64.encodeBase64String(entry.getValue());
            arguments.addProperty(entry.getKey(), base64Encoded);
        }

        for (Map.Entry<String, Object[]> entry : nonByteArrayArguments.entrySet())
            InvokerUtilities.populateWithArray(entry.getKey(), entry.getValue(), arguments);

        return arguments;
    }

    /**
     * Add the given primitive to the arguments.
     * @param key The argument's name.
     * @param value The argument itself.
     * @param dest The arguments to be passed to the serverless name node.
     */
    private void packagePrimitive(String key, Object value, JsonObject dest) {
        if (value instanceof String)
            dest.addProperty(key, (String)value);
        else if (value instanceof Number)
            dest.addProperty(key, (Number)value);
        else if (value instanceof Boolean)
            dest.addProperty(key, (Boolean)value);
        else if (value instanceof Character)
            dest.addProperty(key, (Character)value);
        else
            throw new IllegalArgumentException("Value is not of a valid primitive type. Value's type: "
                    + value.getClass().getSimpleName());
    }
}
