package lab.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.DirectSlice;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.junit.Assert;

public class RocksDBTest {

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @Test
    public void testIterate() throws IOException, RocksDBException {
        File dbPath = tf.newFolder();
        Options dbOpts = new Options();
        dbOpts.setCreateIfMissing(true);
        RocksDB db = RocksDB.open(dbOpts, dbPath.getAbsolutePath());
        ByteBuffer k = ByteBuffer.allocateDirect(4);
        k.putInt(1);
        k.flip();

        ByteBuffer v = ByteBuffer.allocateDirect(4);
        v.putInt(2);
        v.flip();

        try (WriteOptions options = new WriteOptions();
            WriteBatch batch = new WriteBatch()) {
            batch.put(k.slice(), v.slice());
            db.write(options, batch);
        }

        byte[] keyBuf = new byte[k.remaining()];
        k.slice().get(keyBuf);
        byte[] value = db.get(keyBuf);
        Assert.assertNotNull(value);

        try (ReadOptions readOpts = new ReadOptions()) {
            byte[] endKey = new byte[v.remaining()];
            v.slice().get(endKey);
            readOpts.setIterateUpperBound(new Slice(endKey));
            RocksIterator iterator = db.newIterator(readOpts);
            iterator.seek(k.slice());
            Assert.assertTrue(iterator.isValid());
        }

        db.closeE();
        RocksDB.destroyDB(dbPath.getAbsolutePath(), dbOpts);
        dbOpts.close();
    }

    @Test
    public void testIterateWithDirectSlice() throws IOException, RocksDBException {
        File dbPath = tf.newFolder();
        Options dbOpts = new Options();
        dbOpts.setCreateIfMissing(true);
        RocksDB db = RocksDB.open(dbOpts, dbPath.getAbsolutePath());
        ByteBuffer k = ByteBuffer.allocateDirect(4);
        k.putInt(1);
        k.flip();

        ByteBuffer v = ByteBuffer.allocateDirect(4);
        v.putInt(2);
        v.flip();

        try (WriteOptions options = new WriteOptions();
            WriteBatch batch = new WriteBatch()) {
            batch.put(k.slice(), v.slice());
            db.write(options, batch);
        }

        byte[] keyBuf = new byte[k.remaining()];
        k.slice().get(keyBuf);
        byte[] value = db.get(keyBuf);
        Assert.assertNotNull(value);

        try (ReadOptions readOpts = new ReadOptions()) {
            readOpts.setIterateUpperBound(new DirectSlice(v.slice()));
            RocksIterator iterator = db.newIterator(readOpts);
            iterator.seek(k.slice());
            Assert.assertTrue(iterator.isValid());
        }

        db.closeE();
        RocksDB.destroyDB(dbPath.getAbsolutePath(), dbOpts);
        dbOpts.close();
    }
}
