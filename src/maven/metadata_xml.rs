#![allow(non_snake_case)]


pub struct Metadata {
    groupId: String,
    artifactId: String,
    versioning: Versioning,
    version: String,
    plugins: Plugins,
}

pub struct Versioning {
    latest: String,
    release: String,
    versions: Versions,
    lastUpdated: String,
    snapshot: Snapshot,
    snapshotVersions: SnapshotVersions,
}

pub struct Versions {
    version: Vec<String>,
}

pub struct Snapshot {
    timestamp: String,
    buildNumber: Option<u32>,
    //TODO localCopy?
}

pub struct SnapshotVersions {
    snapshotVersion: Vec<SnapshotVersion>,
}

pub struct SnapshotVersion {
    classifier: Option<String>,
    extension: String,
    value: String,
    updated: String,
}

pub struct Plugins {
    plugin: Vec<Plugin>,
}

pub struct Plugin {
    name: Option<String>,
    prefix: Option<String>,
    artifactId: String,
}