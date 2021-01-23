package space.wudi.learncache.zookeeper.util;

import java.io.*;

public interface MySerializable extends Serializable {

    default byte[] toBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(this);
        return baos.toByteArray();
    }

    static <M> M fromBytes(byte[] bytes, Class<M> clazz) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        return clazz.cast(ois.readObject());
    }
}
