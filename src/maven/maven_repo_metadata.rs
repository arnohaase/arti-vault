use std::time::Instant;
use crate::maven::coordinates::{MavenArtifactId, MavenArtifactRef, MavenClassifier, MavenCoordinates, MavenGroupId};


pub enum ArtifactStatus {
    Materialized,
    AnnouncedByUpstream,
    FailedToGetFromUpstream(Instant),
}


/// This trait is designed as a cleaned-up abstraction of the maven-metadata.xml file format
///  described at https://maven.apache.org/ref/3.9.5/maven-repository-metadata/repository-metadata.html
pub trait MavenRepoMetaData {
    fn get_child_groups(&self, group_id: &MavenGroupId) -> Vec<MavenGroupId>;
    fn get_artifacts(&self, group_id: &MavenGroupId) -> Vec<MavenArtifactId>;

    /// NB: this means the versions exist for *any* classifier
    fn get_versions(&self, group_id: &MavenGroupId, artifact_id: &MavenArtifactId) -> Vec<MavenCoordinates>;

    fn get_classifiers(&self, coordinates: &MavenCoordinates) -> Vec<MavenClassifier>;

    fn get_status(&self, coordinates: &MavenCoordinates) -> ArtifactStatus;     //TODO ?!

    //TODO versioning in maven-metadata.xml (https://maven.apache.org/ref/3.9.5/maven-repository-metadata/repository-metadata.html)
    // refers to versions (latest, release) of artifacts, ignoring classifiers -> how to reconcile the two concepts?

    //TODO snapshot build numbers -> are they even used any longer? How are they assigned / maintained?

    //TODO there are two concepts of versions for snapshots - with and without the 'extension'
    // -> make MavenVersion an enum, with 'Release', 'Snapshot' and 'SnapshotWithExtension'?

    //TODO access statistics
    //TODO plugins
}

