

use anyhow::anyhow;
use lazy_static::lazy_static;
use regex::Regex;
use crate::maven::coordinates::*;

lazy_static! {
    static ref TIMESTAMP_REGEX: Regex = Regex::new(r"-\d{8}\.\d{6}").unwrap();
}


pub fn as_maven_path(artifact_ref: &MavenArtifactRef) -> String {
    let version_string = match &artifact_ref.coordinates.version {
        MavenVersion::Release(s) => s,
        MavenVersion::Snapshot { version, .. } => version,
    };

    format!(
        "{}/{}/{}/{}",
        artifact_ref.coordinates.group_id.0.replace('.', "/"),
        artifact_ref.coordinates.artifact_id.0,
        version_string,
        maven_file_name(artifact_ref),
    )
}


fn parse_maven_filename<'a>(file_name: &'a str, artifact_id: &str, version_string: &str) -> anyhow::Result<ParseFilenameResult<'a>> {
    let full_file_name = file_name;
    if file_name.len() < artifact_id.len() + version_string.len() + 2 {
        return Err(anyhow!("not a valid maven file name: {}", full_file_name));
    }

    if !file_name.starts_with(artifact_id) {
        return Err(anyhow!("{} is not a valid maven file name: expected to start with artifact id {}", full_file_name, artifact_id));
    }
    let file_name = &file_name[artifact_id.len()+1 ..];

    if !file_name.starts_with(version_string) {
        return Err(anyhow!("{} is not a valid maven file name: expected to have version string {}", full_file_name, version_string));
    }
    let file_name = &file_name[version_string.len() ..];

    let (file_name, extension) = if let Some(last_dot) = file_name.rfind('.') {
        (&file_name[..last_dot], &file_name[last_dot..])
    }
    else {
        (file_name, "")
    };

    if version_string.contains("-SNAPSHOT") {
        // <artifactId>-<version>-<classifier>-<timestamp>-<buildNumber>.<extension>

        if !version_string.ends_with("-SNAPSHOT") {
            return Err(anyhow!("a snapshot version string should be unqualified - was {}", version_string));
        }

        // NB: classifier is optional and can contain any number of '-' characters
        // NB: build number is optional

        let (classifier, timestamp, build_number) = match parse_classifier_and_timestamp(file_name, full_file_name) {
            Ok((classifier, timestamp)) => {
                (classifier, timestamp, None)
            }
            Err(_) => {
                // try the last segment as a build number
                if let Some(last_dash) = file_name.rfind('-') {
                    let build_number = file_name[last_dash+1..].parse::<u32>()?;
                    let (classifier, timestamp) = parse_classifier_and_timestamp(&file_name[..last_dash], full_file_name)?;
                    (classifier, timestamp, Some(build_number))
                }
                else {
                    return Err(anyhow!("snapshot file name does not end in build number or timestamp: {}", full_file_name));
                }
            }
        };

        Ok(ParseFilenameResult {
            version: MavenVersion::Snapshot {
                version: version_string.to_string(),
                timestamp: timestamp.to_string(),
                build_number,
            },
            classifier,
            extension,
        })
    }
    else {
        //  <artifactId>-<version>-<classifier>.<extension>

        let classifier = if file_name.is_empty() {
            None
        }
        else {
            println!("{:?}", file_name);

            if file_name.starts_with('-') {
                Some(&file_name[1..])
            }
            else {
                return Err(anyhow!("not a valid maven file name - invalid classifier format: {}", full_file_name));
            }
        };

        Ok(ParseFilenameResult {
            version: MavenVersion::Release(version_string.to_string()),
            classifier,
            extension,
        })
    }
}

fn parse_classifier_and_timestamp<'a> (file_name: &'a str, full_file_name: &str) -> anyhow::Result<(Option<&'a str>, &'a str)> {
    if file_name.len() < 16 {
        return Err(anyhow!("snapshot without timestamp: {}", full_file_name));
    }

    let (raw_classifier, time_stamp) = if TIMESTAMP_REGEX.is_match(&file_name[file_name.len() - 16..]) {
        (&file_name[..file_name.len() - 16], &file_name[file_name.len() - 15..])
    }
    else {
        return Err(anyhow!("snapshot without timestamp: {}", full_file_name));
    };

    let classifier = if raw_classifier.starts_with('-') {
        Some(&raw_classifier[1..])
    }
    else {
        None
    };

    Ok((classifier, time_stamp))
}

/// path is the relative path inside a maven repository, i.e. it starts with something like
///  "org/..." or "com/..."
/// The second part of the returned pair is the filename
pub fn parse_maven_path(path: &str) -> anyhow::Result<MavenArtifactRef> {
    //TODO unit test

    if let Some(last_slash) = path.rfind('/') {
        let (without_filename, file_name) = path.split_at(last_slash);

        if let Some(last_slash) = without_filename.rfind('/') {
            let (without_version, version) = without_filename.split_at(last_slash);
            let version = &version[1..];

            if let Some(last_slash) = without_version.rfind('/') {
                let (group_id, artifact_id) = without_version.split_at(last_slash);
                let artifact_id = &artifact_id[1..];

                let parsed_filename = parse_maven_filename(file_name, artifact_id, version)?;

                return Ok(MavenArtifactRef {
                    coordinates: MavenCoordinates {
                        group_id: MavenGroupId(group_id.replace('/', ".")),
                        artifact_id: MavenArtifactId(artifact_id.to_string()),
                        version: parsed_filename.version,
                    },
                    classifier: match parsed_filename.classifier {
                        None => MavenClassifier::Unclassified,
                        Some(s) => MavenClassifier::Classified(s.to_string()),
                    },
                    file_extension: parsed_filename.extension.to_string(),
                });
            }
        }
    }

    Err(anyhow::Error::msg(format!("not a valid Maven artifact path: {:?}", path)))
}



fn maven_file_name(artifact_ref: &MavenArtifactRef) -> String {
    let classifier_string = match &artifact_ref.classifier {
        MavenClassifier::Unclassified => "".to_string(),
        MavenClassifier::Classified(c) => format!("-{}", c),
    };

    match &artifact_ref.coordinates.version {
        MavenVersion::Release(v) => {
            format!("{}-{}{}{}",
                    artifact_ref.coordinates.artifact_id.0,
                    v,
                    classifier_string,
                    artifact_ref.file_extension,
            )
        }
        MavenVersion::Snapshot { version, timestamp, build_number } => {
            let build_number_string = match build_number {
                None => "".to_string(),
                Some(n) => format!("-{}", n),
            };

            format!("{}-{}-SNAPSHOT{}-{}{}{}",
                    artifact_ref.coordinates.artifact_id.0,
                    version,
                    classifier_string,
                    timestamp,
                    build_number_string,
                    artifact_ref.file_extension,
            )
        }
    }
}



#[derive(Debug, Eq, PartialEq)]
struct ParseFilenameResult<'a> {
    version: MavenVersion,
    classifier: Option<&'a str>,
    extension: &'a str, // including leading '.', e.g. ".jar"
}

#[cfg(test)]
mod test {
    use rstest::*;
    use super::*;
    use crate::maven::coordinates::*;

    #[rstest]
    #[case::release("a-1.0.0.jar", "a", "1.0.0", Some(ParseFilenameResult{ version: MavenVersion::Release("1.0.0".to_string()), classifier: None, extension: ".jar"} ))]
    #[case::release_with_dash("x-y-1.0.0.jar", "x-y", "1.0.0", Some(ParseFilenameResult{ version: MavenVersion::Release("1.0.0".to_string()), classifier: None, extension: ".jar"} ))]
    #[case::release_version_with_dash_prefix("x-y-1.0.0.jar", "x", "y-1.0.0", Some(ParseFilenameResult{ version: MavenVersion::Release("y-1.0.0".to_string()), classifier: None, extension: ".jar"} ))]
    #[case::release_version_with_dash_suffix("x-1.0.0-y.jar", "x", "1.0.0-y", Some(ParseFilenameResult{ version: MavenVersion::Release("1.0.0-y".to_string()), classifier: None, extension: ".jar"} ))]
    #[case::release_extension("q-1.0.0.abc", "q", "1.0.0", Some(ParseFilenameResult{ version: MavenVersion::Release("1.0.0".to_string()), classifier: None, extension: ".abc"} ))]
    #[case::release_classifier("a-1.0.0-cla.jar", "a", "1.0.0", Some(ParseFilenameResult{ version: MavenVersion::Release("1.0.0".to_string()), classifier: Some("cla"), extension: ".jar"} ))]
    #[case::release_classifier_with_dash("a-1.0.0-cla-rst.jar", "a", "1.0.0", Some(ParseFilenameResult{ version: MavenVersion::Release("1.0.0".to_string()), classifier: Some("cla-rst"), extension: ".jar"} ))]
    #[case::release_classifier_with_dash_suffix("a-1.0.0-cla-rst.jar", "a", "1.0.0-cla", Some(ParseFilenameResult{ version: MavenVersion::Release("1.0.0-cla".to_string()), classifier: Some("rst"), extension: ".jar"} ))]
    #[case::release_invalid_too_short_1("xxxxxx", "a", "1.0.0", None)]
    #[case::release_invalid_too_short_2("", "a", "1.0.0", None)]
    #[case::release_invalid_wrong_artifact("a-1.0.0.jar", "b", "1.0.0", None)]
    #[case::release_invalid_no_dash_after_artifact("a1.0.0.jar", "a", "1.0.0", None)]
    #[case::release_invalid_wrong_version("a-1.0.0.jar", "a", "1.0.1", None)]
    #[case::release_invalid_no_version("a.jar", "a", "1.0.0", None)]
    #[case::release_invalid_no_dash_before_classifier("a-1.0.0xyz.jar", "a", "1.0.0", None)]

    #[case::snapshot("a-1.0.0-SNAPSHOT-12345678.123456.jar", "a", "1.0.0-SNAPSHOT", Some(ParseFilenameResult{ version: MavenVersion::Snapshot { version: "1.0.0-SNAPSHOT".to_string(), timestamp: "12345678.123456".to_string(), build_number: None }, classifier: None, extension: ".jar"}))]
    #[case::snapshot_build_number("a-1.0.0-SNAPSHOT-12345678.123456-5.jar", "a", "1.0.0-SNAPSHOT", Some(ParseFilenameResult{ version: MavenVersion::Snapshot { version: "1.0.0-SNAPSHOT".to_string(), timestamp: "12345678.123456".to_string(), build_number: Some(5) }, classifier: None, extension: ".jar"}))]
    #[case::snapshot_classifier("a-1.0.0-SNAPSHOT-cla-12345678.123456-5.jar", "a", "1.0.0-SNAPSHOT", Some(ParseFilenameResult{ version: MavenVersion::Snapshot { version: "1.0.0-SNAPSHOT".to_string(), timestamp: "12345678.123456".to_string(), build_number: Some(5) }, classifier: Some("cla"), extension: ".jar"}))]
    #[case::snapshot_classifier_build_number("a-1.0.0-SNAPSHOT-xyz-12345678.123456-5.jar", "a", "1.0.0-SNAPSHOT", Some(ParseFilenameResult{ version: MavenVersion::Snapshot { version: "1.0.0-SNAPSHOT".to_string(), timestamp: "12345678.123456".to_string(), build_number: Some(5) }, classifier: Some("xyz"), extension: ".jar"}))]
    #[case::snapshot_classifier_like_timestamp("a-1.0.0-SNAPSHOT-11111111.111111-22222222.222222-5.jar", "a", "1.0.0-SNAPSHOT", Some(ParseFilenameResult{ version: MavenVersion::Snapshot { version: "1.0.0-SNAPSHOT".to_string(), timestamp: "22222222.222222".to_string(), build_number: Some(5) }, classifier: Some("11111111.111111"), extension: ".jar"}))]
    #[case::snapshot_classifier_with_dash("a-1.0.0-SNAPSHOT-a-b-c-22222222.222222-5.jar", "a", "1.0.0-SNAPSHOT", Some(ParseFilenameResult{ version: MavenVersion::Snapshot { version: "1.0.0-SNAPSHOT".to_string(), timestamp: "22222222.222222".to_string(), build_number: Some(5) }, classifier: Some("a-b-c"), extension: ".jar"}))]
    #[case::snapshot_without_timestamp("a-1.0.0-SNAPSHOT.jar", "a", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_without_timestamp_but_classifier("a-1.0.0-SNAPSHOT-a-b-c.jar", "a", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_without_timestamp_but_classifier_and_build_number("a-1.0.0-SNAPSHOT-a-b-c-5.jar", "a", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_invalid_too_short_1("xxxxxxxxxxxxxxx", "a", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_invalid_too_short_2("", "a", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_invalid_wrong_artifact("a-1.0.0-SNAPSHOT-11111111.222222.jar", "b", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_invalid_no_dash_after_artifact("a1.0.0-SNAPSHOT-11111111.222222.jar", "a", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_invalid_wrong_version("a-1.0.0-SNAPSHOT-11111111.222222.jar", "a", "1.0.1-SNAPSHOT", None)]
    #[case::snapshot_invalid_no_version("a.jar", "a", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_invalid_no_version("a.jar", "a", "1.0.0-SNAPSHOT", None)]
    #[case::snapshot_invalid_build_number("a-1.0.0-SNAPSHOT-12345678.123456-a.jar", "a", "1.0.0-SNAPSHOT", None)]

    #[case::snapshot_lowercase_snapshot("a-1.0.0-snapshot-12345678.123456-a.jar", "a", "1.0.0-snapshot", Some(ParseFilenameResult{ version: MavenVersion::Release("1.0.0-snapshot".to_string()), classifier: Some("12345678.123456-a"), extension: ".jar"}))]
    fn test_parse_filename(#[case] file_name: &str, #[case] artifact_id: &str, #[case] version_string: &str, #[case] expected: Option<ParseFilenameResult>) {
        let actual = parse_maven_filename(file_name, artifact_id, version_string);

        if let Some(expected) = expected {
            let actual = actual.unwrap();
            assert_eq!(actual, expected);
        }
        else {
            assert!(actual.is_err());
        }
    }
}