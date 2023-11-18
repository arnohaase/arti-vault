use lazy_static::lazy_static;
use regex::Regex;


#[derive(Debug, Eq, PartialEq, Clone)]
pub enum MavenVersion {
    Release(String),
    Snapshot {
        version: String,
        timestamp: String,
        build_number: Option<u32>,
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
    // pub file_name: String,
    pub classifier: MavenClassifier,
    pub file_extension: String,
}


#[derive(Debug, Eq, PartialEq)]
struct ParseFilenameResult<'a> {
    version: MavenVersion,
    classifier: Option<&'a str>,
    extension: &'a str, // including leading '.', e.g. ".jar"
}
