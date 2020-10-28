//! Implement services required by the node plugin
//! find volumes provisioned by Mayastor
//! freeze and unfreeze filesystem volumes provisioned by Mayastor
use crate::{
    dev::{Device, DeviceError},
    findmnt,
    mount,
};
use snafu::{ResultExt, Snafu};
use tokio::process::Command;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ServiceError {
    #[snafu(display("Cannot find volume: volume ID: {}", volid))]
    VolumeNotFound { volid: String },
    #[snafu(display("Invalid volume ID: {}, {}", volid, source))]
    InvalidVolumeId {
        source: uuid::parser::ParseError,
        volid: String,
    },
    #[snafu(display("fsfreeze failed: volume ID: {}, {}", volid, error))]
    FsfreezeFailed { volid: String, error: String },
    #[snafu(display("Internal failure: volume ID:{}, {}", volid, source))]
    InternalFailure { source: DeviceError, volid: String },
    #[snafu(display("IO error: volume ID: {}, {}", volid, source))]
    IOError {
        source: std::io::Error,
        volid: String,
    },
    #[snafu(display("Inconsistent mount filesystems: volume ID: {}", volid))]
    InconsistentMountFs { volid: String },
    #[snafu(display("Not a filesystem mount: volume ID: {}", volid))]
    BlockDeviceMount { volid: String },
}

pub enum TypeOfMount {
    FileSystem,
    RawBlock,
}

const FSFREEZE: &str = "fsfreeze";

async fn fsfreeze(
    volume_id: &str,
    freeze_op: &str,
) -> Result<(), ServiceError> {
    let uuid = Uuid::parse_str(volume_id).context(InvalidVolumeId {
        volid: volume_id.to_string(),
    })?;

    if let Some(device) =
        Device::lookup(&uuid).await.context(InternalFailure {
            volid: volume_id.to_string(),
        })?
    {
        let device_path = device.devname();
        if let Some(mnt) = mount::find_mount(Some(&device_path), None) {
            let args = [freeze_op, &mnt.dest];
            let output =
                Command::new(FSFREEZE).args(&args).output().await.context(
                    IOError {
                        volid: volume_id.to_string(),
                    },
                )?;
            return if output.status.success() {
                Ok(())
            } else {
                Err(ServiceError::FsfreezeFailed {
                    volid: volume_id.to_string(),
                    error: String::from_utf8(output.stderr).unwrap(),
                })
            };
        } else {
            let mountpaths = findmnt::get_mountpaths(&device_path).context(
                InternalFailure {
                    volid: volume_id.to_string(),
                },
            )?;
            // if mount::find_mount didn't return any matches,
            // but findmnt did, then the volume is a raw block
            // volume.
            if !mountpaths.is_empty() {
                return Err(ServiceError::BlockDeviceMount {
                    volid: volume_id.to_string(),
                });
            }
        }
    }
    Err(ServiceError::VolumeNotFound {
        volid: volume_id.to_string(),
    })
}

pub async fn freeze_volume(volume_id: &str) -> Result<(), ServiceError> {
    fsfreeze(volume_id, "--freeze").await
}

pub async fn unfreeze_volume(volume_id: &str) -> Result<(), ServiceError> {
    fsfreeze(volume_id, "--unfreeze").await
}

pub async fn find_volume(volume_id: &str) -> Result<TypeOfMount, ServiceError> {
    let uuid = Uuid::parse_str(volume_id).context(InvalidVolumeId {
        volid: volume_id.to_string(),
    })?;

    if let Some(device) =
        Device::lookup(&uuid).await.context(InternalFailure {
            volid: volume_id.to_string(),
        })?
    {
        let device_path = device.devname();
        let mountpaths =
            findmnt::get_mountpaths(&device_path).context(InternalFailure {
                volid: volume_id.to_string(),
            })?;
        debug!("mountpaths : {:?}", mountpaths);
        if !mountpaths.is_empty() {
            let fstype = mountpaths[0].fstype.clone();
            for devmount in mountpaths {
                if fstype != devmount.fstype {
                    // This failure is very unlikely but include for
                    // completeness
                    return Err(ServiceError::InconsistentMountFs {
                        volid: volume_id.to_string(),
                    });
                }
            }
            if fstype == "devtmpfs" {
                return Ok(TypeOfMount::RawBlock);
            } else {
                return Ok(TypeOfMount::FileSystem);
            }
        }
    }
    Err(ServiceError::VolumeNotFound {
        volid: volume_id.to_string(),
    })
}
