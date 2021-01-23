package space.wudi.learncache.zookeeper.util;

import java.io.*;

public interface MySerializable extends Serializable {

    /**
     * serialize this to byte[]
     * @return the byte array represent this
     * @throws IOException when fail to output stream
     */
    default byte[] toBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(this);
        return baos.toByteArray();
    }

    /**
     *
     * @param bytes the array of bytes to be deserialized
     * @param clazz target class type
     * @param <M> target class type
     * @return  target object of clazz
     * @throws IOException when fail to input stream
     * @throws ClassNotFoundException Class of a serialized object cannot be found.
     */
    static <M> M fromBytes(byte[] bytes, Class<M> clazz) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        return clazz.cast(ois.readObject());
    }
}
