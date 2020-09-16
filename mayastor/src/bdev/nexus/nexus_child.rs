use std::{convert::TryFrom, fmt::Display, sync::Arc};

use nix::errno::Errno;
use serde::{export::Formatter, Serialize};
use snafu::{ResultExt, Snafu};

use spdk_sys::{spdk_bdev_module_release_bdev, spdk_io_channel};

use crate::{
    bdev::{
        nexus::{
            nexus_child::ChildState::Faulted,
            nexus_child_status_config::ChildStatusConfig,
        },
        NexusErrStore,
    },
    core::{Bdev, BdevHandle, CoreError, Descriptor, DmaBuf},
    nexus_uri::{bdev_destroy, NexusBdevError},
    rebuild::{ClientOperations, RebuildJob},
    subsys::Config,
};
use std::cell::RefCell;

#[derive(Debug, Snafu)]
pub enum ChildError {
    #[snafu(display("Child is not offline"))]
    ChildNotOffline {},
    #[snafu(display("Child is not closed"))]
    ChildNotClosed {},
    #[snafu(display("Child is faulted, it cannot be reopened"))]
    ChildFaulted {},
    #[snafu(display(
        "Child is smaller than parent {} vs {}",
        child_size,
        parent_size
    ))]
    ChildTooSmall { child_size: u64, parent_size: u64 },
    #[snafu(display("Open child"))]
    OpenChild { source: CoreError },
    #[snafu(display("Claim child"))]
    ClaimChild { source: Errno },
    #[snafu(display("Child is closed"))]
    ChildClosed {},
    #[snafu(display("Invalid state of child"))]
    ChildInvalid {},
    #[snafu(display("Opening child bdev without bdev pointer"))]
    OpenWithoutBdev {},
    #[snafu(display("Failed to create a BdevHandle for child"))]
    HandleCreate { source: CoreError },
}

#[derive(Debug, Snafu)]
pub enum ChildIoError {
    #[snafu(display("Error writing to {}: {}", name, source))]
    WriteError { source: CoreError, name: String },
    #[snafu(display("Error reading from {}: {}", name, source))]
    ReadError { source: CoreError, name: String },
    #[snafu(display("Invalid descriptor for child bdev {}", name))]
    InvalidDescriptor { name: String },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
/// RPC related ingore for internal state.
pub enum ChildStatus {
    /// available for RW
    Online,
    /// temporarily unavailable for R, out of sync with nexus (needs rebuild)
    Degraded,
    /// permanently unavailable for RW
    Faulted,
}
#[derive(Debug, Serialize, PartialEq, Deserialize, Copy, Clone)]
pub(crate) enum Reason {
    /// no particular reason for the child to be in this state
    /// this is typically the init state. It is safe to assume the
    /// state changed to have any reason at any give time
    Undefined,
    /// out of sync - needs to be rebuilt
    OutOfSync,
    /// can not open
    CantOpen,
}

impl Display for Reason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Undefined => write!(f, "Unknown"),
            Self::OutOfSync => {
                write!(f, "The child is out of sync and requires a rebuild")
            }
            Self::CantOpen => write!(f, "Failed to open the child bdev"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) enum ChildState {
    /// child has not been opened, but we are in the process of opening it
    Init,
    /// cannot add this bdev to the parent as its incompatible property wise
    ConfigInvalid,
    /// the child is open for RW
    Open,
    /// the child has been closed by the nexus
    Closed,
    /// the child is faulted
    Faulted(Reason),
}

impl Display for ChildState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Faulted(r) => write!(f, "faulted with reason {}", r),
            Self::Init => write!(f, "Init"),
            Self::ConfigInvalid => write!(f, "Config parameters are invalid"),
            Self::Open => write!(f, "child is open"),
            Self::Closed => write!(f, "closed"),
        }
    }
}

/// structure used to track the internal state of the child
#[derive(Debug)]
struct State {
    inner: ChildState,
    reason: Reason,
}

impl ToString for ChildStatus {
    fn to_string(&self) -> String {
        match *self {
            ChildStatus::Degraded => "degraded",
            ChildStatus::Faulted => "faulted",
            ChildStatus::Online => "online",
        }
        .parse()
        .unwrap()
    }
}

#[derive(Debug, Serialize)]
pub struct NexusChild {
    /// name of the parent this child belongs too
    pub(crate) parent: String,
    /// Name of the child is the URI used to create it.
    /// Note that bdev name can differ from it!
    pub(crate) name: String,
    #[serde(skip_serializing)]
    /// the bdev wrapped in Bdev
    pub(crate) bdev: Option<Bdev>,
    #[serde(skip_serializing)]
    /// channel on which we submit the IO
    pub(crate) ch: *mut spdk_io_channel,
    #[serde(skip_serializing)]
    pub(crate) desc: Option<Arc<Descriptor>>,
    /// current state of the child
    #[serde(skip_serializing)]
    state: RefCell<State>,
    /// descriptor obtained after opening a device
    #[serde(skip_serializing)]
    pub(crate) bdev_handle: Option<BdevHandle>,
    /// record of most-recent IO errors
    #[serde(skip_serializing)]
    pub(crate) err_store: Option<NexusErrStore>,
}

impl Display for NexusChild {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        if self.bdev.is_some() {
            let bdev = self.bdev.as_ref().unwrap();
            writeln!(
                f,
                "{}: {:?}/{:?}, blk_cnt: {}, blk_size: {}",
                self.name,
                self.state,
                self.status(),
                bdev.num_blocks(),
                bdev.block_len(),
            )
        } else {
            writeln!(
                f,
                "{}: state {:?}/{:?}",
                self.name,
                self.state,
                self.status()
            )
        }
    }
}

impl NexusChild {
    fn set_state(&mut self, state: ChildState) {
        trace!(
            "{}: child {}: state change from {} to {}",
            self.parent,
            self.name,
            self.state.borrow().inner.to_string(),
            state.to_string(),
        );

        self.state.borrow_mut().inner = state;
    }

    /// Open the child in RW mode and claim the device to be ours. If the child
    /// is already opened by someone else (i.e one of the targets) it will
    /// error out.
    ///
    /// only devices in the closed or Init state can be opened.
    ///
    /// A child can only be opened if:
    ///  - its not faulted
    ///  - if its not already opened
    pub(crate) fn open(
        &mut self,
        parent_size: u64,
    ) -> Result<String, ChildError> {
        trace!("{}: Opening child device {}", self.parent, self.name);

        // verify that valid status of the child before we open it
        match self.status() {
            ChildState::Faulted(reason) => {
                error!(
                    "{}: can not open child {} reason {}",
                    self.parent, self.name, reason
                );
                return Err(ChildError::ChildFaulted {});
            }
            ChildState::Open => {
                // the child (should) already be open
                assert_eq!(self.bdev.is_some(), true);
            }
            _ => {}
        }

        let bdev = self.bdev.as_ref().unwrap();

        let child_size = bdev.size_in_bytes();
        if parent_size > child_size {
            error!(
                "{}: child {} too small, parent size: {} child size: {}",
                self.parent, self.name, parent_size, child_size
            );

            self.set_state(ChildState::ConfigInvalid);
            return Err(ChildError::ChildTooSmall {
                parent_size,
                child_size,
            });
        }

        let desc = Arc::new(Bdev::open_by_name(&bdev.name(), true).map_err(
            |source| {
                self.set_state(Faulted(Reason::CantOpen));
                ChildError::OpenChild {
                    source,
                }
            },
        )?);

        self.bdev_handle = Some(BdevHandle::try_from(desc.clone()).unwrap());
        self.desc = Some(desc);

        let cfg = Config::get();
        if cfg.err_store_opts.enable_err_store {
            self.err_store =
                Some(NexusErrStore::new(cfg.err_store_opts.err_store_size));
        };

        self.set_state(ChildState::Open);

        debug!("{}: child {} opened successfully", self.parent, self.name);
        NexusChild::save_state_change();
        Ok(self.name.clone())
    }

    /// Fault the child with an optional specific reason. Because fault has
    /// multiple variants we have a helper method to do this
    fn fault(&mut self, reason: Option<Reason>) {
        self._close();
        if let Some(r) = reason {
            self.set_state(ChildState::Faulted(r));
        } else {
            self.set_state(ChildState::Faulted(Reason::Undefined));
        }
        NexusChild::save_state_change();
    }

    /// Set the child as out of sync with the nexus
    /// It requires a full rebuild before it can service IO
    /// and remains degraded until such time
    /// TODO: rivisit callsite, perhaps rework
    pub(crate) fn out_of_sync(&mut self, out_of_sync: bool) {
        if out_of_sync {
            self.fault(Some(Reason::OutOfSync));
        }
    }
    /// Set the child as temporarily offline
    /// TODO: channels need to be updated when bdevs are closed
    pub(crate) fn offline(&mut self) {
        self.close();
    }

    /// Online a previously offlined child
    /// TODO: channels need to be updated when bdevs are closed
    pub(crate) fn online(
        &mut self,
        parent_size: u64,
    ) -> Result<String, ChildError> {
        self.open(parent_size)
    }

    /// Save the state of the children to the config file
    pub(crate) fn save_state_change() {
        if ChildStatusConfig::save().is_err() {
            error!("Failed to save child status information");
        }
    }

    /// returns the state of the child
    pub fn status(&self) -> ChildState {
        self.state.borrow().inner
    }

    pub(crate) fn rebuilding(&self) -> bool {
        match RebuildJob::lookup(&self.name) {
            Ok(_) => self.status() == ChildState::Faulted(Reason::OutOfSync),
            Err(_) => false,
        }
    }

    /// return a descriptor to this child
    pub fn get_descriptor(&self) -> Result<Arc<Descriptor>, CoreError> {
        if let Some(ref d) = self.desc {
            Ok(d.clone())
        } else {
            Err(CoreError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }

    /// closed the descriptor and handle, does not destroy the bdev
    fn _close(&mut self) {
        trace!("{}: Closing child {}", self.parent, self.name);
        if let Some(bdev) = self.bdev.as_ref() {
            unsafe {
                if !(*bdev.as_ptr()).internal.claim_module.is_null() {
                    spdk_bdev_module_release_bdev(bdev.as_ptr());
                }
            }
        }
        // just to be explicit
        let hdl = self.bdev_handle.take();
        let desc = self.desc.take();
        drop(hdl);
        drop(desc);
    }

    /// close the bdev -- we have no means of determining if this succeeds
    pub(crate) fn close(&mut self) -> ChildState {
        self._close();
        self.set_state(ChildState::Closed);
        NexusChild::save_state_change();
        ChildState::Closed
    }

    /// create a new nexus child
    pub fn new(name: String, parent: String, bdev: Option<Bdev>) -> Self {
        NexusChild {
            name,
            bdev,
            parent,
            desc: None,
            ch: std::ptr::null_mut(),
            state: RefCell::new(State {
                inner: ChildState::Init,
                reason: Reason::Undefined,
            }),
            bdev_handle: None,
            err_store: None,
        }
    }

    /// destroy the child bdev
    pub(crate) async fn destroy(&mut self) -> Result<(), NexusBdevError> {
        trace!("destroying child {:?}", self);
        assert_eq!(self.status(), ChildState::Closed);
        if let Some(_bdev) = &self.bdev {
            bdev_destroy(&self.name).await
        } else {
            warn!("Destroy child without bdev");
            Ok(())
        }
    }

    /// returns if a child can be written to
    pub fn can_rw(&self) -> bool {
        self.status() == ChildState::Open
    }

    /// return references to child's bdev and descriptor
    /// both must be present - otherwise it is considered an error
    pub fn get_dev(&self) -> Result<(&Bdev, &BdevHandle), ChildError> {
        if !self.can_rw() {
            info!("{}: Closed child: {}", self.parent, self.name);
            return Err(ChildError::ChildClosed {});
        }

        if let Some(bdev) = &self.bdev {
            if let Some(desc) = &self.bdev_handle {
                return Ok((bdev, desc));
            }
        }

        Err(ChildError::ChildInvalid {})
    }

    /// write the contents of the buffer to this child
    pub async fn write_at(
        &self,
        offset: u64,
        buf: &DmaBuf,
    ) -> Result<usize, ChildIoError> {
        match self.bdev_handle.as_ref() {
            Some(desc) => {
                Ok(desc.write_at(offset, buf).await.context(WriteError {
                    name: self.name.clone(),
                })?)
            }
            None => Err(ChildIoError::InvalidDescriptor {
                name: self.name.clone(),
            }),
        }
    }

    /// read from this child device into the given buffer
    pub async fn read_at(
        &self,
        offset: u64,
        buf: &mut DmaBuf,
    ) -> Result<u64, ChildIoError> {
        match self.bdev_handle.as_ref() {
            Some(desc) => {
                Ok(desc.read_at(offset, buf).await.context(ReadError {
                    name: self.name.clone(),
                })?)
            }
            None => Err(ChildIoError::InvalidDescriptor {
                name: self.name.clone(),
            }),
        }
    }

    /// Return the rebuild job which is rebuilding this child, if rebuilding
    fn get_rebuild_job(&self) -> Option<&mut RebuildJob> {
        let job = RebuildJob::lookup(&self.name).ok()?;
        assert_eq!(job.nexus, self.parent);
        Some(job)
    }

    /// Return the rebuild progress on this child, if rebuilding
    pub fn get_rebuild_progress(&self) -> i32 {
        self.get_rebuild_job()
            .map(|j| j.stats().progress as i32)
            .unwrap_or_else(|| -1)
    }
}
