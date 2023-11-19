
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum MavenVersion {
    Release(String),
    Snapshot {
        version: String, // ending in '-SNAPSHOT'
        timestamp: String,
        build_number: Option<u32>,
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct MavenArtifactId(pub String);

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct MavenGroupId(pub String);

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct MavenCoordinates {
    pub group_id: MavenGroupId,
    pub artifact_id: MavenArtifactId,
    pub version: MavenVersion,
}

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub enum MavenClassifier {
    Unclassified,
    Classified(String),
}

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct MavenArtifactRef {
    pub coordinates: MavenCoordinates,
    // pub file_name: String,
    pub classifier: MavenClassifier,
    pub file_extension: String,
}

