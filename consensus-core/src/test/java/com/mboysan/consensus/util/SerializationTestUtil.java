package com.mboysan.consensus.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class SerializationTestUtil {
    private SerializationTestUtil() {
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T serializeDeserialize(T obj) throws IOException, ClassNotFoundException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            oos.flush();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                 ObjectInputStream ois = new ObjectInputStream(bais)) {
                return (T) ois.readObject();
            }
        }
    }

    public static void assertSerialized(Serializable expected, Serializable actual) {
        // this is a workaround to instead of overriding hashcode and equals for all the classes.
        assertEquals(expected.toString(), actual.toString());
    }
}
