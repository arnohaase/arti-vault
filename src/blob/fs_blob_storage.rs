use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use tokio::fs::{create_dir_all, OpenOptions, read_dir, remove_dir, remove_file, rename, try_exists};
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
        data: impl Stream<Item=Bytes> + Send
    ) -> anyhow::Result<PathBuf> {
        let mut data = Box::pin(data);

        //TODO trace
        //TODO performance / monitoring

        let mut data_path = directory_path;
        data_path.push("data");

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

        let mut temp_directory_path = directory_path.clone();
        temp_directory_path.pop();
        temp_directory_path.push(format!("{}.inserting", key.as_hyphenated()));

        create_dir_all(&temp_directory_path).await?;

        let result = match Self::do_insert(directory_path.clone(), data).await {
            Ok(_) => {
                rename(temp_directory_path, directory_path).await?;
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
        result
    }

    async fn get(&self, key: &Uuid) -> anyhow::Result<Option<Box<dyn Stream<Item=std::io::Result<Bytes>>>>> {
        let directory_path = self.directory_path_for_key(key);
        trace!("getting file system blob {} from directory {}", key.as_hyphenated(), directory_path.display());

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