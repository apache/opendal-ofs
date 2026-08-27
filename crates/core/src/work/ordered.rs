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

//! Constant-memory cursors over ordered record streams.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::marker::PhantomData;

use crate::Error;

pub(crate) trait OrderedRead {
    type Item;

    fn next(&mut self) -> Result<Option<Self::Item>, Error>;
}

pub(crate) trait AsyncOrderedRead {
    type Item;

    fn next(&mut self) -> impl Future<Output = Result<Option<Self::Item>, Error>>;
}

/// Remove adjacent duplicate keys from an ordered input.
pub(crate) struct Unique<R, F, K> {
    source: R,
    key: F,
    previous: Option<K>,
}

impl<R, F, K> Unique<R, F, K> {
    pub(crate) const fn new(source: R, key: F) -> Self {
        Self {
            source,
            key,
            previous: None,
        }
    }
}

impl<R, F, K> OrderedRead for Unique<R, F, K>
where
    R: OrderedRead,
    F: Fn(&R::Item) -> K,
    K: Eq,
{
    type Item = R::Item;

    fn next(&mut self) -> Result<Option<Self::Item>, Error> {
        while let Some(item) = self.source.next()? {
            let key = (self.key)(&item);
            if self.previous.as_ref() == Some(&key) {
                continue;
            }
            self.previous = Some(key);
            return Ok(Some(item));
        }
        Ok(None)
    }
}

/// The next item from a merge join of two ordered inputs.
pub(crate) enum JoinItem<L, R> {
    Left(L),
    Match(L, R),
    Right(R),
}

/// Merge two ordered inputs without retaining either input.
///
/// The left input is a lookup set and must have unique keys. A matching left
/// item is retained while right items with the same key stream past it, so one
/// stored fact can resolve any number of demands. If either side is exhausted,
/// the cursor naturally forwards the other side.
pub(crate) struct OrderedJoin<L, R, FL, FR, K>
where
    L: OrderedRead,
    R: OrderedRead,
{
    left: L,
    right: R,
    left_key: FL,
    right_key: FR,
    left_head: Option<L::Item>,
    right_head: Option<R::Item>,
    left_matched: bool,
    marker: PhantomData<K>,
}

impl<L, R, FL, FR, K> OrderedJoin<L, R, FL, FR, K>
where
    L: OrderedRead,
    R: OrderedRead,
    FL: Fn(&L::Item) -> K,
    FR: Fn(&R::Item) -> K,
    L::Item: Clone,
    K: Ord,
{
    pub(crate) fn new(left: L, right: R, left_key: FL, right_key: FR) -> Self {
        Self {
            left,
            right,
            left_key,
            right_key,
            left_head: None,
            right_head: None,
            left_matched: false,
            marker: PhantomData,
        }
    }

    pub(crate) fn next(&mut self) -> Result<Option<JoinItem<L::Item, R::Item>>, Error> {
        loop {
            if self.left_head.is_none() {
                self.left_head = self.left.next()?;
                self.left_matched = false;
            }
            if self.right_head.is_none() {
                self.right_head = self.right.next()?;
            }
            let ordering = match (&self.left_head, &self.right_head) {
                (Some(left), Some(right)) => (self.left_key)(left).cmp(&(self.right_key)(right)),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => return Ok(None),
            };
            match ordering {
                Ordering::Less if self.left_matched => {
                    self.left_head = None;
                }
                Ordering::Less => {
                    return Ok(Some(JoinItem::Left(
                        self.left_head.take().expect("left merge item exists"),
                    )));
                }
                Ordering::Equal => {
                    self.left_matched = true;
                    return Ok(Some(JoinItem::Match(
                        self.left_head
                            .as_ref()
                            .expect("left merge item exists")
                            .clone(),
                        self.right_head.take().expect("right merge item exists"),
                    )));
                }
                Ordering::Greater => {
                    return Ok(Some(JoinItem::Right(
                        self.right_head.take().expect("right merge item exists"),
                    )));
                }
            }
        }
    }
}

/// Merge any number of ordered inputs while retaining one item per input.
pub(crate) struct OrderedMerge<R, T, K, N, F> {
    readers: Vec<R>,
    next: N,
    key: F,
    pending: OrderedQueue<T, K>,
}

fn read_next<R: OrderedRead>(reader: &mut R) -> Result<Option<R::Item>, Error> {
    reader.next()
}

impl<R, K, F> OrderedMerge<R, R::Item, K, fn(&mut R) -> Result<Option<R::Item>, Error>, F>
where
    R: OrderedRead,
    K: Ord,
    F: Fn(&R::Item) -> Result<K, Error>,
{
    pub(crate) fn from_readers(readers: Vec<R>, key: F) -> Result<Self, Error> {
        Self::new(readers, read_next::<R>, key)
    }
}

impl<R, T, K, N, F> OrderedMerge<R, T, K, N, F>
where
    K: Ord,
    N: FnMut(&mut R) -> Result<Option<T>, Error>,
    F: Fn(&T) -> Result<K, Error>,
{
    pub(crate) fn new(mut readers: Vec<R>, mut next: N, key: F) -> Result<Self, Error> {
        let mut pending = OrderedQueue::new();
        for (source, reader) in readers.iter_mut().enumerate() {
            if let Some(value) = next(reader)? {
                pending.push(source, key(&value)?, value);
            }
        }
        Ok(Self {
            readers,
            next,
            key,
            pending,
        })
    }

    pub(crate) fn peek_key(&self) -> Option<&K> {
        self.pending.peek_key()
    }

    pub(crate) fn next(&mut self) -> Result<Option<T>, Error> {
        Ok(self.next_with_source()?.map(|(_, value)| value))
    }

    pub(crate) fn next_with_source(&mut self) -> Result<Option<(usize, T)>, Error> {
        let Some((source, value)) = self.pending.pop() else {
            return Ok(None);
        };
        if let Some(value) = (self.next)(&mut self.readers[source])? {
            self.pending.push(source, (self.key)(&value)?, value);
        }
        Ok(Some((source, value)))
    }

    /// Consume the next equal-key group into caller-owned reducer state.
    pub(crate) fn reduce_next_group<S>(
        &mut self,
        mut state: S,
        mut reduce: impl FnMut(&mut S, usize, T) -> Result<(), Error>,
    ) -> Result<Option<(K, S)>, Error>
    where
        K: Eq,
    {
        let Some((source, value)) = self.next_with_source()? else {
            return Ok(None);
        };
        let key = (self.key)(&value)?;
        reduce(&mut state, source, value)?;
        while self.peek_key() == Some(&key) {
            let (source, value) = self
                .next_with_source()?
                .expect("peeked ordered item remains available");
            reduce(&mut state, source, value)?;
        }
        Ok(Some((key, state)))
    }
}

/// Merge asynchronous ordered inputs with the same group semantics as
/// [`OrderedMerge`].
pub(crate) struct AsyncOrderedMerge<R, K, F>
where
    R: AsyncOrderedRead,
{
    readers: Vec<R>,
    key: F,
    pending: OrderedQueue<R::Item, K>,
}

impl<R, K, F> AsyncOrderedMerge<R, K, F>
where
    R: AsyncOrderedRead,
    K: Ord,
    F: Fn(&R::Item) -> Result<K, Error>,
{
    pub(crate) async fn from_readers(mut readers: Vec<R>, key: F) -> Result<Self, Error> {
        let mut pending = OrderedQueue::new();
        for (source, reader) in readers.iter_mut().enumerate() {
            if let Some(value) = reader.next().await? {
                pending.push(source, key(&value)?, value);
            }
        }
        Ok(Self {
            readers,
            key,
            pending,
        })
    }

    async fn next_with_source(&mut self) -> Result<Option<(usize, R::Item)>, Error> {
        let Some((source, value)) = self.pending.pop() else {
            return Ok(None);
        };
        if let Some(value) = self.readers[source].next().await? {
            self.pending.push(source, (self.key)(&value)?, value);
        }
        Ok(Some((source, value)))
    }

    pub(crate) async fn reduce_next_group<S>(
        &mut self,
        mut state: S,
        mut reduce: impl FnMut(&mut S, usize, R::Item) -> Result<(), Error>,
    ) -> Result<Option<(K, S)>, Error>
    where
        K: Eq,
    {
        let Some((source, value)) = self.next_with_source().await? else {
            return Ok(None);
        };
        let key = (self.key)(&value)?;
        reduce(&mut state, source, value)?;
        while self.pending.peek_key() == Some(&key) {
            let (source, value) = self
                .next_with_source()
                .await?
                .expect("peeked ordered item remains available");
            reduce(&mut state, source, value)?;
        }
        Ok(Some((key, state)))
    }
}

/// One source-aware minimum queue shared by synchronous and asynchronous
/// ordered merges.
pub(crate) struct OrderedQueue<T, K>(BinaryHeap<OrderedItem<T, K>>);

impl<T, K: Ord> OrderedQueue<T, K> {
    pub(crate) const fn new() -> Self {
        Self(BinaryHeap::new())
    }

    pub(crate) fn push(&mut self, source: usize, key: K, value: T) {
        self.0.push(OrderedItem { key, value, source });
    }

    pub(crate) fn peek_key(&self) -> Option<&K> {
        self.0.peek().map(|item| &item.key)
    }

    pub(crate) fn pop(&mut self) -> Option<(usize, T)> {
        self.0.pop().map(|item| (item.source, item.value))
    }
}

struct OrderedItem<T, K> {
    key: K,
    value: T,
    source: usize,
}

impl<T, K: Ord> PartialEq for OrderedItem<T, K> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source == other.source
    }
}

impl<T, K: Ord> Eq for OrderedItem<T, K> {}

impl<T, K: Ord> PartialOrd for OrderedItem<T, K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, K: Ord> Ord for OrderedItem<T, K> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source.cmp(&self.source))
    }
}
