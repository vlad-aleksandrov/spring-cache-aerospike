/*
 * Copyright 2015 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package us.swcraft.springframework.store.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.iq80.snappy.SnappyFramedInputStream;
import org.iq80.snappy.SnappyFramedOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.swcraft.springframework.store.StoreCompression;

import java.io.*;


/**
 * Kryo serializer.
 *
 * @param <T>
 */
public class KryoSerializer<T> implements Serializer<T> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Compression type. Default is {@link StoreCompression#NONE}.
     */
    private StoreCompression compressionType = StoreCompression.NONE;

    private KryoPool pool;

    public KryoSerializer() {
        init();
    }

    public KryoSerializer(final StoreCompression compressionType) {
        this.compressionType = compressionType;
        init();
    }

    protected void init() {
        KryoFactory factory = new KryoFactory() {
            public Kryo create() {
                Kryo kryo = getKryoInstance();
                customConfiguration(kryo);
                return kryo;
            }
        };
        pool = new KryoPool.Builder(factory).softReferences().build();
    }

    /**
     * Configure kryo instance, customize settings.
     * 
     * @param kryo
     */
    public void customConfiguration(Kryo kryo) {
        // NO-OP
    }

    public Kryo getKryoInstance() {
        return new Kryo();
    }

    @Override
    public byte[] serialize(final T data) throws SerializationException {
        final Kryo kryo = pool.borrow();
        try (
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(8192);
                final OutputStream compressionOutputStream = wrapOutputStream(outputStream);
                final Output output = new Output(compressionOutputStream);) {

            kryo.writeObject(output, data);
            output.flush();
            compressionOutputStream.flush();
            outputStream.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            log.error("Serialization error: {}", e.getMessage());
            log.trace("", e);
            throw new SerializationException(data.getClass() + " serialization problem", e);
        } finally {
            pool.release(kryo);
        }

    }

    @Override
    public T deserialize(final byte[] serializedData, final Class<T> type) throws SerializationException {
        final Kryo kryo = pool.borrow();
        try (
                final ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedData);
                final InputStream decompressionInputStream = wrapInputStream(inputStream);
                final Input input = new Input(decompressionInputStream);) {

            final T result = kryo.readObject(input, type);
            return result;
        } catch (Exception e) {
            log.error("Deserialization error: {}", e.getMessage());
            log.trace("", e);
            throw new SerializationException(type + " deserialization problem", e);
        } finally {
            pool.release(kryo);
        }

    }

    private OutputStream wrapOutputStream(final OutputStream os) throws IOException {
        switch (compressionType) {
            case SNAPPY:
                return new SnappyFramedOutputStream(os);
            default:
                return new BufferedOutputStream(os);
        }
    }

    private InputStream wrapInputStream(final InputStream is) throws IOException {
        switch (compressionType) {
            case SNAPPY:
                return new SnappyFramedInputStream(is, false);
            default:
                return new BufferedInputStream(is);
        }
    }

}
