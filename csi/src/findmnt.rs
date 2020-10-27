use crate::error::DeviceError;
use serde_json::Value;
use std::{collections::HashMap, process::Command, string::String, vec::Vec};

// Keys of interest we expect to find in the JSON output generated
/// by findmnt.
const TARGET_KEY: &str = "target";
const SOURCE_KEY: &str = "source";
const FSTYPE_KEY: &str = "fstype";

#[derive(Debug)]
pub struct DeviceMount {
    pub mount_path: String,
    pub fstype: String,
}

#[derive(Debug)]
struct FindmntFilter<'a> {
    key: &'a str,
    value: &'a str,
}

impl PartialEq<Value> for FindmntFilter<'_> {
    /// Special case the comparison for the source field returned
    /// by findmnt.
    fn eq(&self, value: &Value) -> bool {
        if self.key == SOURCE_KEY {
            if let Some(strvalue) = value.as_str() {
                let devpath = convert_findmnt_devicepath(strvalue);
                if devpath == self.value {
                    return true;
                }
            } else {
                error!("value for {} is not a string", self.key);
            }
        }
        self.value == value
    }
}

/// The source field returned from findmnt and
///   can be different for the same source on different systems,
///   for example
///   dev[/nvme0n1], udev[/nvme0n1], tmpfs[/nvme0n1], devtmpfs[/nvme0n1]
///   Convert this to the expected /dev/nvme0n1 and added to the hashmap
fn convert_findmnt_devicepath(devpath: &str) -> String {
    lazy_static! {
        static ref RE_UDEVPATH: regex::Regex = regex::Regex::new(
            r"(?x).*\[(?P<device>/.*)\]
        ",
        )
        .unwrap();
    }
    match RE_UDEVPATH.captures(devpath) {
        Some(caps) => format!("/dev{}", &caps["device"]),
        _ => devpath.to_string(),
    }
}

/// Convert the json map entry to a hashmap of strings
/// The source field returned from findmnt is converted
/// to the /dev/xxx form if required.
fn jsonmap_to_hashmap(
    json_map: &serde_json::Map<String, Value>,
) -> HashMap<String, String> {
    let mut hmap: HashMap<String, String> = HashMap::new();
    for (key, value) in json_map {
        if let Some(strvalue) = value.as_str() {
            if key == SOURCE_KEY {
                hmap.insert(
                    key.to_string(),
                    convert_findmnt_devicepath(strvalue),
                );
            } else {
                hmap.insert(key.to_string(), strvalue.to_string());
            }
        } else {
            //FIXME: key:value pairs are discarded if the value is not a
            // string.
            error!("value for {} is not a string", key);
        }
    }
    hmap
}

/// This function recurses over the de-serialised JSON returned by findmnt,
/// finding entries which have key-pair's matching the filter key-pair,
/// and populates a vector with the values for the item_key.
///
/// For Mayastor usage the assumptions made on the structure are:
///  1. An object has keys named "target" and "source" for a mount point.
///  2. An object may contain nested arrays of objects.
///
/// The search is deliberately generic (and hence slower) in an attempt to
/// be more robust to future changes in findmnt.
fn filter_findmnt(
    json_val: &serde_json::value::Value,
    filter: &FindmntFilter,
    results: &mut Vec<HashMap<String, String>>,
) {
    if let Some(json_array) = json_val.as_array() {
        for jsonvalue in json_array {
            filter_findmnt(&jsonvalue, filter, results);
        }
    }
    if let Some(json_map) = json_val.as_object() {
        if let Some(value) = json_map.get(filter.key) {
            if filter == value {
                results.push(jsonmap_to_hashmap(json_map));
            }
        }
        // If the object has arrays, then the assumption is that they are arrays
        // of objects.
        for (_, jsonvalue) in json_map {
            if jsonvalue.is_array() {
                filter_findmnt(jsonvalue, filter, results);
            }
        }
    }
}

/// findmnt executable name.
const FINDMNT: &str = "findmnt";
/// findmnt arguments, we only want source, target and filesystem type fields.
const FINDMNT_ARGS: [&str; 3] = ["-J", "-o", "SOURCE,TARGET,FSTYPE"];

/// Execute the Linux utility findmnt, collect the json output,
/// invoke the filter function and return the filtered results.
fn findmnt(
    params: FindmntFilter,
) -> Result<Vec<HashMap<String, String>>, DeviceError> {
    let output = Command::new(FINDMNT).args(&FINDMNT_ARGS).output()?;
    if output.status.success() {
        let json_str = String::from_utf8(output.stdout)?;
        let json: Value = serde_json::from_str(&json_str)?;
        let mut results: Vec<HashMap<String, String>> = Vec::new();
        filter_findmnt(&json, &params, &mut results);
        Ok(results)
    } else {
        Err(DeviceError {
            message: String::from_utf8(output.stderr)?,
        })
    }
}

/// Use the Linux utility findmnt to find the name of the device mounted at a
/// directory or block special file, if any.
/// mount_path is the path a device is mounted on.
pub(crate) fn findmnt_get_devicepath(
    mount_path: &str,
) -> Result<Option<String>, DeviceError> {
    let tgt_filter = FindmntFilter {
        key: TARGET_KEY,
        value: mount_path,
    };
    match findmnt(tgt_filter) {
        Ok(sources) => {
            match sources.len() {
                0 => Ok(None),
                1 => {
                    if let Some(devicepath) = sources[0].get(SOURCE_KEY) {
                        Ok(Some(devicepath.to_string()))
                    } else {
                        Err(DeviceError {
                            message: "missing source field".to_string(),
                        })
                    }
                }
                _ => {
                    // should be impossible ...
                    warn!(
                        "multiple sources mounted on target {:?}->{}",
                        sources, mount_path
                    );
                    Err(DeviceError {
                        message: format!(
                            "multiple devices mounted at {}",
                            mount_path
                        ),
                    })
                }
            }
        }
        Err(e) => Err(e),
    }
}

/// Use the Linux utility findmnt to find the mount paths for a block device,
/// if any.
/// device_path is the path to the device for example "/dev/sda1"
pub(crate) fn findmnt_get_mountpaths(
    device_path: &str,
) -> Result<Vec<DeviceMount>, DeviceError> {
    let dev_filter = FindmntFilter {
        key: SOURCE_KEY,
        value: device_path,
    };
    match findmnt(dev_filter) {
        Ok(results) => {
            let mut mountpaths: Vec<DeviceMount> = Vec::new();
            for entry in results {
                if let Some(mountpath) = entry.get(TARGET_KEY) {
                    if let Some(fstype) = entry.get(FSTYPE_KEY) {
                        mountpaths.push(DeviceMount {
                            mount_path: mountpath.to_string(),
                            fstype: fstype.to_string(),
                        })
                    } else {
                        error!("Missing fstype for {}", mountpath);
                        mountpaths.push(DeviceMount {
                            mount_path: mountpath.to_string(),
                            fstype: "unspecified".to_string(),
                        })
                    }
                } else {
                    warn!("missing target field {:?}", entry);
                }
            }
            Ok(mountpaths)
        }
        Err(e) => Err(e),
    }
}
