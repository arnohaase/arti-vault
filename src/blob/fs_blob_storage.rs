use std::path::PathBuf;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use tokio::fs::{create_dir_all, metadata, OpenOptions, read_dir, remove_dir, remove_file, rename, try_exists};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tracing::{error, trace};
use uuid::Uuid;

use crate::blob::blob_storage::BlobStorage;

pub struct FsBlobStorage {
    root: PathBuf,
}
impl FsBlobStorage {
    fn directory_path_for_key(&self, key: &Uuid) -> PathBuf { //TODO unit test
        let mut result = self.root.clone();

        let key_string = key.as_hyphenated().to_string();
        // first level only single character to facilitate sharding
        result.push(&key_string[0..1]);
        result.push(&key_string[1..4]);
        result.push(&key_string[4..6]);
        result.push(&key_string[6..8]);
        result.push(key_string);

        result
    }

    async fn do_insert(
        directory_path: PathBuf,
        file_name_suffix: &str,
        data: impl Stream<Item=Bytes> + Send
    ) -> anyhow::Result<PathBuf> {
        let mut data = Box::pin(data);

        //TODO trace
        //TODO performance / monitoring

        //TODO temporary 'dirty' marker in the file system

        let mut data_path = directory_path;
        data_path.push(format!("data{}", file_name_suffix));

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&data_path)
            .await?;

        loop {
            match data.next().await {
                Some(bytes) => {
                    file.write(&bytes).await?;
                }
                None =>
                    break,
            }
        }

        Ok(data_path)
    }

    async fn create_lock(directory: &PathBuf) -> anyhow::Result<PathBuf> {
        let mut path = directory.clone();
        path.push(".lock");

        if try_exists(&path).await? {
            let lock_metadata = metadata(path).await?;
            let lock_time = lock_metadata.created()?; //TODO better write the application's timestamp to the file than use the file system's?
            let is_timed_out = match lock_time.elapsed() {
                Ok(duration) => duration.as_secs() > 5*60, //TODO make this configurable
                Err(_) => false, // lock timestamp is in the future
            };

            if is_timed_out {
                //TODO clean up / self heal
                todo!()
            }
            else {
                return Err(anyhow!("locked / concurrent access"));
            }
        }

        OpenOptions::new()
            .create_new(true)
            .open(&path)
            .await?;

        Ok(path)
    }
}


#[async_trait]
impl BlobStorage<Uuid> for FsBlobStorage {
    //TODO metadata - inserted, last updated, last read (access statistics in general)
    // SHA1, MD5
    // 'completed' -> to mark this as actually referenced from the outside (?)
    // 'fsck'

    async fn insert(&self, data: impl Stream<Item=Bytes> + Send) -> anyhow::Result<Uuid> {
        //TODO performance / monitoring
        let key = Uuid::new_v4();
        let directory_path = self.directory_path_for_key(&key);
        trace!("inserting file blob - synthetic key is {}, directory is {}", key.as_hyphenated(), directory_path.display());
        create_dir_all(&directory_path).await?;

        let lock_file = Self::create_lock(&directory_path).await?;

        let result = match Self::do_insert(directory_path, "", data).await {
            Ok(_) => {
                Ok(key)
            }
            Err(e) => {
                match self.delete(&key).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("error cleaning up directory for key {} after failed attempt to insert: {}", &key, e);
                    }
                }
                Err(e)
            }
        };

        // no point in propagating a failure to delete the lock file up the call stack
        let _ = remove_file(&lock_file).await;
        result
    }

    async fn update(&self, key: &Uuid, data: impl Stream<Item=Bytes> + Send) -> anyhow::Result<bool> {
        let directory_path = self.directory_path_for_key(key);
        trace!("updating file system blob {} in directory {}", key.as_hyphenated(), directory_path.display());

        if !try_exists(&directory_path).await? {
            return Ok(false);
        }

        let lock_file = Self::create_lock(&directory_path).await?;

        //TODO reject if there is a lock
        //TODO self-heal if there is an old lock

        // Start by writing the new blob data to a separate file, suffixed ".new".
        // The idea is to minimize the time of inconsistent state in the file system, and allow full
        //  recoverability.
        let new_path = match Self::do_insert(directory_path.clone(), ".new", data).await {
            Ok(p) => p,
            Err(e) => {
                // roll back failed changes as best we can
                let mut new_file_path = directory_path;
                new_file_path.push("data.new");
                let _ = remove_file(new_file_path).await;
                let _ = remove_file(lock_file).await;
                return Err(e);
            }
        };

        let mut bak_path = directory_path.clone();
        bak_path.push("data.orig");
        let mut data_path = directory_path.clone();
        data_path.push("data");

        //TODO
        // NB: From this point on, failure requires manual intervention (or at least invasive automation)

        // move original blob data to "data.orig"...
        rename(&data_path, &bak_path).await?;

        // ... and "data.new" to "data".
        rename(&new_path, data_path).await?;

        let _ = remove_file(lock_file).await;
        Ok(true)
    }

    async fn get(&self, key: &Uuid) -> anyhow::Result<Option<Box<dyn Stream<Item=std::io::Result<Bytes>>>>> {
        let directory_path = self.directory_path_for_key(key);
        trace!("getting file system blob {} from directory {}", key.as_hyphenated(), directory_path.display());

        //TODO there is a small but non-empty window during an update when the 'get' does not find a data file -> handle

        let mut data_path = directory_path;
        data_path.push("data");

        if !try_exists(&data_path).await? {
            return Ok(None);
        }

        let file = OpenOptions::new()
            .read(true)
            .open(data_path)
            .await?;

        let stream = ReaderStream::new(file);
        Ok(Some(Box::new(stream)))
    }

    async fn delete(&self, key: &Uuid) -> anyhow::Result<bool> {
        let directory_path = self.directory_path_for_key(key);
        trace!("deleting file system blob {} from directory {}", key.as_hyphenated(), directory_path.display());
        if try_exists(&directory_path).await? {
            // First, atomically rename the directory by adding ".deleting" as a suffix so that
            //  partial deletes do not leave inconsistent state.
            //
            // NB: This "deleting" directory can not exist due to a previous attempt at deleting
            //  because there UUIDs are unique
            //
            // NB: This is racy with concurrent reads and can cause spurious failure in them

            let mut temp_path = directory_path.clone();
            temp_path.pop();
            temp_path.push(format!("{}.deleting", key.as_hyphenated()));

            rename(&directory_path, &temp_path).await?;

            let mut files = read_dir(&temp_path).await?;
            loop {
                match files.next_entry().await? {
                    Some(dir_entry) => {
                        // If there is an entry that is not a file, or that is not removable, this
                        //  returns an error.
                        remove_file(&dir_entry.path()).await?;
                    }
                    None => {
                        break;
                    }
                }
            }

            remove_dir(temp_path).await?;
            Ok(true)
        }
        else {
            Ok(false)
        }
    }
}