use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tokio::fs::{create_dir_all, OpenOptions, read_dir, remove_dir, remove_file, rename, try_exists};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use tracing::{error, trace};
use uuid::Uuid;

use crate::blob::blob_storage::{BlobStorage, RetrievedBlob};

#[derive(Serialize, Deserialize)]
struct BlobMetaData {
    sha1: [u8;20],
    md5: [u8;16],
}


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

        let mut data_path = directory_path.clone();
        data_path.push("data");

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&data_path)
            .await?;

        let mut sha1_hasher: Sha1 = Default::default();
        let mut md5_hasher = md5::Context::new();

        loop {
            match data.next().await {
                Some(bytes) => {
                    sha1_hasher.update(&bytes);
                    md5_hasher.consume(&bytes);
                    file.write(&bytes).await?;
                }
                None =>
                    break,
            }
        }

        let metadata = BlobMetaData {
            sha1: sha1_hasher.finalize().into(),
            md5: md5_hasher.compute().into(),
        };

        let metadata_json = serde_json::to_string(&metadata)?;

        let mut metadata_file = directory_path;
        metadata_file.push("metadata.json");

        let mut metadata_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .append(true)
            .open(metadata_file)
            .await?;
        metadata_file.write_all(metadata_json.as_bytes())
            .await?;

        Ok(data_path)
    }
}


#[async_trait]
impl BlobStorage<Uuid> for FsBlobStorage {
    //TODO 'fsck'

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

    async fn get(&self, key: &Uuid) -> anyhow::Result<Option<RetrievedBlob>> {
        let directory_path = self.directory_path_for_key(key);
        trace!("getting file system blob {} from directory {}", key.as_hyphenated(), directory_path.display());

        let mut data_path = directory_path.clone();
        data_path.push("data");

        if !try_exists(&data_path).await? {
            return Ok(None);
        }

        let file = OpenOptions::new()
            .read(true)
            .open(data_path)
            .await?;

        let stream = ReaderStream::new(file);

        let mut metadata_path = directory_path;
        metadata_path.push("metadata.json");
        let mut metadata_file = OpenOptions::new()
            .read(true)
            .open(metadata_path)
            .await?;

        let mut metadata_json = String::new();
        metadata_file.read_to_string(&mut metadata_json)
            .await?;

        let metadata: BlobMetaData = serde_json::from_str(&&metadata_json)?;

        Ok(Some(RetrievedBlob {
            data: Box::new(stream),
            md5: metadata.md5,
            sha1: metadata.sha1,
        }))
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
