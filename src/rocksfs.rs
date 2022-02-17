use std::path::Path;

use eyre::{eyre, Result};
use rocksdb::{DBPinnableSlice, Options, WriteBatch, DB};

#[derive(Debug)]
pub struct RocksFs {
    db: DB,
}

impl RocksFs {
    pub fn new<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_enable_blob_files(true);
        opts.set_min_blob_size(512 * 1024);

        let db = DB::open(&opts, path)?;

        Ok(RocksFs { db })
    }

    pub fn put<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        Ok(self.db.put(key, value)?)
    }

    pub fn del<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        Ok(self.db.delete(key)?)
    }

    pub fn bulk_put<'b, K, V>(&self, values: impl Iterator<Item = (&'b K, &'b V)>) -> Result<()>
    where
        K: AsRef<[u8]> + 'b,
        V: AsRef<[u8]> + 'b,
    {
        let mut batch = WriteBatch::default();
        for (k, v) in values {
            batch.put(k, v);
        }
        Ok(self.db.write(batch)?)
    }

    pub fn bulk_delete<'b, K>(&self, keys: impl Iterator<Item = &'b K>) -> Result<()>
    where
        K: AsRef<[u8]> + 'b,
    {
        let mut batch = WriteBatch::default();
        for k in keys {
            batch.delete(k);
        }
        Ok(self.db.write(batch)?)
    }

    pub fn get<K>(&self, key: K) -> Result<DBPinnableSlice<'_>>
    where
        K: AsRef<[u8]>,
    {
        let res = self
            .db
            .get_pinned(key)?
            .ok_or_else(|| eyre!("key not found"))?;
        Ok(res)
    }

    pub fn get_size<K>(&self, key: K) -> Result<usize>
    where
        K: AsRef<[u8]>,
    {
        let res = self
            .db
            .get_pinned(key)?
            .ok_or_else(|| eyre!("key not found"))?;
        Ok(res.len())
    }

    pub fn has<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        self.db
            .get_pinned(key)
            .map(|v| v.is_some())
            .map_err(Into::into)
    }

    /// Deletes all elements in the database.
    pub fn clear(&self) -> Result<()> {
        for (key, _) in self.db.full_iterator(rocksdb::IteratorMode::Start) {
            self.db.delete(key)?;
        }

        Ok(())
    }

    pub fn number_of_keys(&self) -> Result<u64> {
        let keys = self
            .db
            .property_int_value("rocksdb.estimate-num-keys")?
            .unwrap_or_default();
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_empty() {
        let dir = tempfile::tempdir().unwrap();

        let rocksfs = RocksFs::new(dir.path()).unwrap();
        assert_eq!(rocksfs.number_of_keys().unwrap(), 0);
    }

    #[test]
    fn test_open_empty() {
        let dir = tempfile::tempdir().unwrap();

        {
            let _rocksfs = RocksFs::new(dir.path()).unwrap();
        }

        {
            let _rocksfs = RocksFs::new(dir.path()).unwrap();
        }
    }

    #[test]
    fn test_put_get_number_of_keys() {
        let dir = tempfile::tempdir().unwrap();
        let rocksfs = RocksFs::new(dir.path()).unwrap();

        for i in 0..10 {
            rocksfs.put(&format!("foo{i}"), [i; 128]).unwrap();
        }

        assert_eq!(rocksfs.number_of_keys().unwrap(), 10);

        for i in 0..10 {
            assert_eq!(&rocksfs.get(&format!("foo{i}")).unwrap()[..], [i; 128]);
            assert_eq!(rocksfs.get_size(&format!("foo{i}")).unwrap(), 128);
        }

        drop(rocksfs);

        // Reread for size
        let rocksfs = RocksFs::new(dir.path()).unwrap();
        assert_eq!(rocksfs.number_of_keys().unwrap(), 10);
    }

    #[test]
    fn test_put_get_del() {
        let dir = tempfile::tempdir().unwrap();
        let rocksfs = RocksFs::new(dir.path()).unwrap();

        for i in 0..10 {
            rocksfs.put(&format!("foo{i}"), [i; 128]).unwrap();
        }

        assert_eq!(rocksfs.number_of_keys().unwrap(), 10);

        for i in 0..10 {
            assert_eq!(&rocksfs.get(&format!("foo{i}")).unwrap()[..], [i; 128]);
        }

        for i in 0..5 {
            rocksfs.del(&format!("foo{}", i)).unwrap();
        }

        assert_eq!(rocksfs.number_of_keys().unwrap(), 5);

        for i in 0..10 {
            if i < 5 {
                assert!(rocksfs.get(&format!("foo{i}")).is_err());
            } else {
                assert_eq!(&rocksfs.get(&format!("foo{i}")).unwrap()[..], [i; 128]);
            }
        }
    }

    #[test]
    fn test_iter() {
        let dir = tempfile::tempdir().unwrap();
        let rocksfs = RocksFs::new(dir.path()).unwrap();

        for i in 0..10 {
            rocksfs.put(&format!("foo{i}"), [i; 128]).unwrap();
        }

        assert_eq!(rocksfs.number_of_keys().unwrap(), 10);

        // for r in rocksfs.iter() {
        //     let (key, value) = r.unwrap();
        //     let i: u8 = key.strip_prefix("foo").unwrap().parse().unwrap();
        //     assert_eq!(value, [i; 128]);
        // }

        // for r in rocksfs.keys() {
        //     let key = r.unwrap();
        //     let i: u8 = key.strip_prefix("foo").unwrap().parse().unwrap();
        //     assert!(i < 10);
        // }

        // for r in rocksfs.values() {
        //     let value = r.unwrap();
        //     assert_eq!(value.len(), 128);
        // }
    }
}
