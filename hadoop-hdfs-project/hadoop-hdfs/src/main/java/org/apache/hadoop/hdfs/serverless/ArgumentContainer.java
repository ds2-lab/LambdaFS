package org.apache.hadoop.hdfs.serverless;

import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kerby.util.Base64;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Serves as a wrapper for passing arguments to serverless functions.
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

    public ArgumentContainer() {
        primitiveArguments = new HashMap<>();
        byteArrayArguments = new HashMap<>();
        nonByteArrayArguments = new HashMap<>();
        objectArguments = new HashMap<>();
    }

    /**
     * Generic catch-all function for adding an argument.
     * @param key The name of the argument.
     * @param value The argument itself.
     */
    public void put(String key, Object value) {
        LOG.debug("Adding arguments. Key: \"" + key + "\", value: " + value.toString() + ", value's class: "
                + value.getClass().getSimpleName());

        Class<?> clazz = value.getClass().getComponentType();

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
        LOG.debug("Adding byte[] argument \"" + key + "\"");
        byteArrayArguments.put(key, value);
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

        LOG.debug("Adding non-byte array argument \"" + key + "\"");

        nonByteArrayArguments.put(key, value);
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
        LOG.debug("Adding primitive argument \"" + key + "\"");
        primitiveArguments.put(key, value);
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

        LOG.debug("Adding object argument \"" + key + "\"");
        objectArguments.put(key, value);
    }

    /**
     * Package the arguments into a JsonObject.
     * @return JsonObject containing all the arguments.
     */
    public JsonObject convertToJsonObject() throws IOException {
        int totalNumArguments = nonByteArrayArguments.size() + byteArrayArguments.size()
                + primitiveArguments.size() + objectArguments.size();

        LOG.debug("Packaging " + totalNumArguments + " arguments into JsonObject.");
        LOG.debug("\tNon-Byte Array Arguments: " + nonByteArrayArguments.size());
        LOG.debug("\tByte Array Arguments: " + byteArrayArguments.size());
        LOG.debug("\tPrimitive Arguments: " + primitiveArguments.size());
        LOG.debug("\tObject Arguments: " + objectArguments.size());

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