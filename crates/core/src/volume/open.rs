// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Create and open a Managed volume from its experimental v0 format.

use std::num::NonZeroUsize;
use std::ops::RangeBounds;

use opendal::Operator;
use tokio::io::AsyncWrite;

use crate::Error;
use crate::ErrorKind;
use crate::authority::{AuthorityAccess, AuthorityObservation, DefaultAuthorityAccess};
use crate::data::{
    CoreDataAccess, DataAccess, ExtentCodec, FilePartitioner, RangeReader, ReusableFile,
    validate_file_map,
};
use crate::filesystem::{ChangeCursor, ContentRef, NodeId, VolumeId};
use crate::format::{
    FORMAT_KEY, FORMAT_RECORD, FileDataLayout, FileExtentMap, GcEpoch, NamespaceCommit,
    NamespaceRevision, VolumeFormat,
};
use crate::storage::ControlRecord;
use crate::work::{WorkBudget, WorkContext};

use super::namespace::{self, Namespace};
use super::publication;

const FORMAT: ControlRecord<VolumeFormat> = ControlRecord::new(FORMAT_KEY, FORMAT_RECORD);

/// User choices that become a persisted `VolumeFormat`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateOptions {
    file_data_layout: FileDataLayout,
    authority: Option<crate::format::ExtensionDescriptor>,
}

impl CreateOptions {
    pub fn new(file_data_layout: FileDataLayout) -> Self {
        Self {
            file_data_layout,
            authority: None,
        }
    }

    pub fn with_authority(mut self, authority: crate::format::ExtensionDescriptor) -> Self {
        self.authority = Some(authority);
        self
    }

    pub const fn file_data_layout(&self) -> &FileDataLayout {
        &self.file_data_layout
    }

    pub fn authority(&self) -> Option<&crate::format::ExtensionDescriptor> {
        self.authority.as_ref()
    }
}

/// Runtime transfer and work budgets for one opened volume.
#[derive(Clone, Copy, Debug)]
pub struct VolumeRuntime {
    transfer_concurrency: NonZeroUsize,
    work_memory_bytes: NonZeroUsize,
    read_gap_bytes: usize,
    multipart_part_bytes: Option<NonZeroUsize>,
}

impl VolumeRuntime {
    pub fn new(
        transfer_concurrency: NonZeroUsize,
        work_memory_bytes: NonZeroUsize,
        read_gap_bytes: usize,
        multipart_part_bytes: Option<NonZeroUsize>,
    ) -> Self {
        Self {
            transfer_concurrency,
            work_memory_bytes,
            read_gap_bytes,
            multipart_part_bytes,
        }
    }

    pub fn standard() -> Self {
        Self::new(
            NonZeroUsize::new(4).expect("nonzero"),
            NonZeroUsize::new(256 * 1024 * 1024).expect("nonzero"),
            1024 * 1024,
            None,
        )
    }
}

/// Statically composed data and namespace-authority access.
pub trait AccessFamily: Clone + Send + Sync + std::fmt::Debug + Unpin + 'static {
    type Data: DataAccess;
    type Authority: AuthorityAccess + Clone;

    fn data(&self) -> &Self::Data;

    fn authority(&self) -> &Self::Authority;
}

/// One Managed access family assembled from independent capabilities.
#[derive(Clone, Debug, Default)]
pub struct ManagedAccess<D, A> {
    data: D,
    authority: A,
}

impl<D, A> ManagedAccess<D, A> {
    pub const fn new(data: D, authority: A) -> Self {
        Self { data, authority }
    }
}

impl<D, A> AccessFamily for ManagedAccess<D, A>
where
    D: DataAccess,
    A: AuthorityAccess + Clone,
{
    type Data = D;
    type Authority = A;

    fn data(&self) -> &Self::Data {
        &self.data
    }

    fn authority(&self) -> &Self::Authority {
        &self.authority
    }
}

/// Core whole-file identity access with the `main` authority.
pub type CoreAccess = ManagedAccess<CoreDataAccess, DefaultAuthorityAccess>;

/// Opened Managed volume facade.
#[derive(Clone)]
pub struct ManagedVolume<A: AccessFamily = CoreAccess> {
    format: VolumeFormat,
    operator: Operator,
    multipart_part_bytes: NonZeroUsize,
    work_budget: WorkBudget,
    stream_concurrency: usize,
    read_gap_bytes: usize,
    access: A,
    authority_name: String,
}

pub struct ManagedObservation {
    pub(crate) namespace: Namespace<FileExtentMap>,
    pub(crate) authority: AuthorityObservation,
    pub(crate) commit: NamespaceCommit,
}

impl ManagedObservation {
    pub const fn namespace(&self) -> &Namespace<FileExtentMap> {
        &self.namespace
    }

    pub const fn authority_id(&self) -> crate::authority::AuthorityId {
        self.authority.id
    }

    pub const fn revision(&self) -> NamespaceRevision {
        self.authority.head.current_commit
    }

    pub fn can_read_revision(&self, revision: NamespaceRevision) -> bool {
        let sequence = revision.change_cursor.sequence();
        let head = self.authority.head;
        sequence >= head.minimum_retained_cursor.sequence()
            && sequence <= head.current_commit.change_cursor.sequence()
    }

    pub const fn gc_epoch(&self) -> GcEpoch {
        self.authority.head.gc_epoch
    }
}

impl<A: AccessFamily> ManagedVolume<A> {
    pub const fn format(&self) -> &VolumeFormat {
        &self.format
    }

    pub const fn id(&self) -> VolumeId {
        self.format.volume_id()
    }

    pub fn authority_name(&self) -> &str {
        &self.authority_name
    }

    pub const fn operator(&self) -> &Operator {
        &self.operator
    }

    pub const fn access(&self) -> &A {
        &self.access
    }

    pub const fn work_budget(&self) -> WorkBudget {
        self.work_budget
    }

    pub const fn stream_concurrency(&self) -> usize {
        self.stream_concurrency
    }

    pub const fn multipart_part_bytes(&self) -> NonZeroUsize {
        self.multipart_part_bytes
    }

    pub const fn read_gap_bytes(&self) -> usize {
        self.read_gap_bytes
    }

    pub(crate) fn file_decoding_count(&self) -> usize {
        self.access.data().decoding_count()
    }

    pub(crate) fn transfer_window_bytes(&self) -> usize {
        self.work_budget
            .memory_target_bytes()
            .saturating_div(self.stream_concurrency)
            .max(1)
    }

    /// Create a volume in empty storage, or reopen it when the same layout exists.
    pub async fn create(
        operator: &Operator,
        options: CreateOptions,
        access: A,
        runtime: VolumeRuntime,
        authority_name: impl Into<String>,
    ) -> Result<Self, Error> {
        require_control_capabilities(operator)?;
        let format = VolumeFormat::new(
            VolumeId::generate(),
            NodeId::generate(),
            options.file_data_layout,
            options.authority,
        );
        if FORMAT.write(operator, &format, None).await? {
            let volume =
                Self::from_parts(format, operator.clone(), access, runtime, authority_name)?;
            volume.initialize().await?;
            return Ok(volume);
        }
        let existing = Self::open(operator, access, runtime, authority_name).await?;
        if existing.format.file_data_layout() != format.file_data_layout()
            || existing.format.authority() != format.authority()
        {
            return Err(Error::conflict(
                "create Managed volume",
                "storage already contains a different volume layout",
            ));
        }
        Ok(existing)
    }

    /// Read the volume format from storage and attach runtime access.
    pub async fn open(
        operator: &Operator,
        access: A,
        runtime: VolumeRuntime,
        authority_name: impl Into<String>,
    ) -> Result<Self, Error> {
        require_control_capabilities(operator)?;
        let observed = FORMAT.read(operator).await?.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "open Managed volume",
                "volume format is missing",
            )
        })?;
        let volume = Self::from_parts(
            observed.value,
            operator.clone(),
            access,
            runtime,
            authority_name,
        )?;
        volume.read_authority().await?;
        Ok(volume)
    }

    fn from_parts(
        format: VolumeFormat,
        operator: Operator,
        access: A,
        runtime: VolumeRuntime,
        authority_name: impl Into<String>,
    ) -> Result<Self, Error> {
        let work_budget = WorkBudget::new(runtime.work_memory_bytes, runtime.transfer_concurrency)?;
        let multipart_part_bytes = runtime.multipart_part_bytes.unwrap_or_else(|| {
            NonZeroUsize::new(
                work_budget
                    .memory_target_bytes()
                    .saturating_div(runtime.transfer_concurrency.get())
                    .max(1),
            )
            .expect("a positive memory target produces a positive stream window")
        });
        validate_access(&format, &access)?;
        Ok(Self {
            format,
            operator,
            multipart_part_bytes,
            work_budget,
            stream_concurrency: runtime.transfer_concurrency.get(),
            read_gap_bytes: runtime.read_gap_bytes,
            access,
            authority_name: authority_name.into(),
        })
    }

    async fn initialize(&self) -> Result<(), Error> {
        let authority = self.access.authority();
        match authority
            .observe(&self.operator, &self.authority_name)
            .await
        {
            Ok(_) => return self.observe().await.map(drop),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let namespace =
            namespace::write_genesis(self, self.format.root_node_id(), GcEpoch::ZERO).await?;
        let commit = NamespaceCommit::genesis(self.id(), namespace);
        let revision = publication::write_commit(self, GcEpoch::ZERO, &commit).await?;
        authority
            .initialize(
                &self.operator,
                self.multipart_part_bytes,
                crate::authority::AuthorityHead {
                    current_commit: revision,
                    gc_epoch: GcEpoch::ZERO,
                    minimum_retained_cursor: ChangeCursor::GENESIS,
                },
            )
            .await
    }

    pub async fn observe(&self) -> Result<ManagedObservation, Error> {
        let workspace = WorkContext::create(self.work_budget)?;
        self.observe_with_base_in(&workspace, None)
            .await
            .map(|(observation, _)| observation)
    }

    pub async fn read_namespace(
        &self,
        revision: NamespaceRevision,
    ) -> Result<Namespace<FileExtentMap>, Error> {
        let workspace = WorkContext::create(self.work_budget)?;
        self.read_namespace_in(&workspace, revision).await
    }

    pub(crate) async fn read_namespace_in(
        &self,
        workspace: &WorkContext,
        revision: NamespaceRevision,
    ) -> Result<Namespace<FileExtentMap>, Error> {
        let commit = publication::read_commit(self, revision).await?;
        namespace::read_views(self, workspace, &[(&commit, revision.change_cursor)])
            .await?
            .pop()
            .ok_or_else(|| Error::corrupt("read Managed namespace", "namespace view is missing"))
    }

    pub(crate) async fn observe_with_base_in(
        &self,
        workspace: &WorkContext,
        base: Option<NamespaceRevision>,
    ) -> Result<(ManagedObservation, Option<Namespace<FileExtentMap>>), Error> {
        let authority = self.read_authority().await?;
        let head = authority.head;
        let commit = publication::read_commit(self, head.current_commit).await?;
        let readable_base = base.filter(|base| {
            let sequence = base.change_cursor.sequence();
            sequence >= head.minimum_retained_cursor.sequence()
                && sequence < head.current_commit.change_cursor.sequence()
        });
        let (namespace, base_namespace) = match readable_base {
            Some(base) => {
                let reference = if base.change_cursor == head.minimum_retained_cursor
                    && base.object.locator.gc_epoch < head.current_commit.object.locator.gc_epoch
                {
                    head.current_commit
                } else {
                    base
                };
                let base_commit = publication::read_commit(self, reference).await?;
                let mut namespaces = namespace::read_views(
                    self,
                    workspace,
                    &[
                        (&base_commit, base.change_cursor),
                        (&commit, commit.change_cursor),
                    ],
                )
                .await?
                .into_iter();
                let base_namespace = namespaces
                    .next()
                    .expect("two requested namespace views include the base");
                let namespace = namespaces
                    .next()
                    .expect("two requested namespace views include the current view");
                (namespace, Some(base_namespace))
            }
            None => {
                let namespace =
                    namespace::read_views(self, workspace, &[(&commit, commit.change_cursor)])
                        .await?
                        .pop()
                        .expect("one requested namespace view is returned");
                (namespace, None)
            }
        };
        Ok((
            ManagedObservation {
                namespace,
                authority,
                commit,
            },
            base_namespace,
        ))
    }

    pub(crate) async fn read_authority(&self) -> Result<AuthorityObservation, Error> {
        self.access
            .authority()
            .observe(&self.operator, &self.authority_name)
            .await
    }

    pub(crate) async fn replace_head(
        &self,
        observed: &AuthorityObservation,
        head: crate::authority::AuthorityHead,
    ) -> Result<bool, Error> {
        self.access
            .authority()
            .compare_exchange(
                &self.operator,
                self.multipart_part_bytes,
                &self.authority_name,
                observed,
                head,
            )
            .await
    }

    pub(crate) async fn read_extent(
        &self,
        reader: &mut RangeReader,
        reference: crate::format::ExtentRef,
        range: std::ops::Range<u64>,
        destination: &mut (impl AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        let destination: &mut (dyn AsyncWrite + Send + Unpin) = destination;
        self.access
            .data()
            .codec()
            .decode(reader, reference, range, destination)
            .await
    }

    pub(crate) async fn read_data<'a>(
        &'a self,
        content: (ContentRef, FileExtentMap),
        range: impl RangeBounds<u64>,
        reusable: Option<ReusableFile<'a>>,
        destination: &'a mut (impl AsyncWrite + Send + Unpin),
    ) -> Result<(), Error> {
        let (content, reference) = content;
        validate_file_map(&reference, content, self.file_decoding_count())?;
        let range = crate::data::logical_range(content.length(), range)?;
        crate::data::restore_file(
            self.access.data(),
            &self.operator,
            self.read_gap_bytes,
            self.transfer_window_bytes(),
            reference,
            content,
            range,
            reusable,
            destination,
        )
        .await
    }
}

fn require_control_capabilities(operator: &Operator) -> Result<(), Error> {
    let capability = operator.info().full_capability();
    if capability.read
        && capability.write
        && capability.write_with_if_not_exists
        && capability.write_with_if_match
    {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::Unsupported,
        "open Managed volume",
        "storage lacks conditional Managed control operations",
    ))
}

fn validate_access(format: &VolumeFormat, access: &impl AccessFamily) -> Result<(), Error> {
    let layout = format.file_data_layout();
    if layout.partitioning() != access.data().partitioner().descriptor()
        || layout.decodings()
            != access
                .data()
                .codec()
                .descriptor()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        || format.authority() != access.authority().info().as_ref()
    {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "open Managed volume",
            "runtime access does not match the persisted format",
        ));
    }
    Ok(())
}
