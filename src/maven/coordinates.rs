use tracing::trace;

pub enum MavenVersion {
    Release(String),
    Snapshot {
        version: String,
        timestamp: String,
        build_number: u32,
    }
}

pub struct MavenArtifactId(pub String);

pub struct MavenGroupId(pub String);

pub struct MavenCoordinates {
    pub group_id: MavenGroupId,
    pub artifact_id: MavenArtifactId,
    pub version: MavenVersion,
}

pub enum MavenClassifier {
    Unclassified,
    Classified(String),
}

pub struct MavenArtifactRef {
    pub coordinates: MavenCoordinates,
    pub file_name: String,
    pub classifier: MavenClassifier,
}
impl MavenArtifactRef {


    //TODO separate this into 'path' logic
    /// path is the relative path inside a maven repository, i.e. it starts with something like
    ///  "org/..." or "com/..."
    /// The second part of the returned pair is the filename
    pub fn parse_path(path: &str) -> anyhow::Result<MavenArtifactRef> { //TODO unit test
    trace!("parsing path {:?}", path);

        if let Some(last_slash) = path.rfind('/') {
            let (without_filename, file_name) = path.split_at(last_slash);
            let file_name_raw = &file_name[1..];

            if let Some(last_slash) = without_filename.rfind('/') {
                let (without_version, version) = without_filename.split_at(last_slash);
                let version = &version[1..];

                if let Some(last_slash) = without_version.rfind('/') {
                    let (group_id, artifact_id) = without_version.split_at(last_slash);
                    let artifact_id = &artifact_id[1..];

                    return Ok(MavenArtifactRef {
                        coordinates: MavenCoordinates {
                            group_id: MavenGroupId(group_id.replace('/', ".")),
                            artifact_id: MavenArtifactId(artifact_id.to_string()),
                            version: MavenVersion::Release(version.to_string()), //TODO snapshot
                        },
                        file_name: file_name_raw.to_string(),
                        classifier: MavenClassifier::Unclassified, //TODO - extract artifact ID from path, and deduce 'trunk' filename, with anything after it version and classifier
                    });
                }
            }
        }

        Err(anyhow::Error::msg(format!("not a valid Maven artifact path: {:?}", path)))
    }

    pub fn as_path(&self) -> String {
        let version_string = match &self.coordinates.version {
            MavenVersion::Release(s) => s,
            MavenVersion::Snapshot { version, .. } => version, //TODO
        };

        format!(
            "{}/{}/{}/{}",
            self.coordinates.group_id.0.replace('.', "/"),
            self.coordinates.artifact_id.0,
            version_string,
            self.file_name,
        )
    }
}
