#
# Copyright 2014 WebFilings, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""This module contains the default functions to use when performing
persistence operations backed by the App Engine ndb library.
"""
import json
import logging
import os
import time

from hash_ring import HashRing

from itertools import imap
from itertools import islice
from itertools import izip

from random import shuffle

from google.appengine.ext import ndb

from furious.context import get_current_context
from furious.context.context import ContextResultBase
from furious import config
from furious import errors


CLEAN_QUEUE = config.get_completion_cleanup_queue()
DEFAULT_QUEUE = config.get_completion_default_queue()
CLEAN_DELAY = config.get_completion_cleanup_delay()
QUEUE_HEADER = 'HTTP_X_APPENGINE_QUEUENAME'

RING_SIZE = 10
SPOTS = range(0, RING_SIZE)
RING = HashRing(SPOTS)


class FuriousContextNotFoundError(Exception):
    """FuriousContext entity not found in the datastore."""


class FuriousContextFailed(ndb.Model):
    """Simple Context to mark if we hit a failure for a given context"""

    @classmethod
    def from_id(cls, id):
        """will build the appropriate key and perform a datastore get"""
        return cls.full_key(id).get()

    @classmethod
    def full_key(cls, id):
        key = cls.parent_key(id)
        return ndb.Key(FuriousContextFailed, id, parent=key)

    @classmethod
    def parent_key(cls, id):
        return FuriousCompletionMarker.full_key(id)

    @classmethod
    def mark_context_failed(context_id):
        """Will create the context failed marker if needed"""

        marker = FuriousContextFailed.from_id(context_id)
        if marker:
            return

        parent_key = FuriousContextFailed.parent_key(context_id)
        return FuriousContextFailed(id=context_id, parent=parent_key)

    @classmethod
    def check_context_failed(cls, context_id):
        """Will use a Keys only ancestor query to see if we've failed or not"""

        key = cls.parent_key(context_id)
        query = FuriousAsyncMarker.query(ancestor=key)

        result = query.fetch(keys_only=True)
        if len(result) > 0:
            return True


class FuriousContext(ndb.Model):
    """NDB entity to store a Furious Context as JSON."""

    context = ndb.JsonProperty(indexed=False, compressed=True)

    @classmethod
    def from_context(cls, context):
        """Create a `cls` entity from a context."""
        key = ndb.Key(FuriousContext, context.id)
        return cls(id=context.id, context=context.to_dict(), parent=key)

    @classmethod
    def from_id(cls, id):
        """Load a `cls` entity and instantiate the Context it stores."""
        from furious.context import Context

        entity = cls.full_key(id).get()

        if not entity:
            raise FuriousContextNotFoundError(
                "Context entity not found for: {}".format(id))

        return Context.from_dict(entity.context)

    @classmethod
    def full_key(cls, id):
        """Build a key for the context so that the context will be in its own
        entity group"""

        key = ndb.Key('FuriousContext', id)
        return ndb.Key('FuriousContext', id, parent=key)


class FuriousAsyncMarker(ndb.Model):
    """This entity serves as a 'complete' marker."""

    result = ndb.JsonProperty(indexed=False, compressed=True)
    status = ndb.IntegerProperty(indexed=False)

    @property
    def success(self):
        from furious.async import AsyncResult
        return self.status != AsyncResult.ERROR

    @classmethod
    def full_key(cls, async_id, context_id):
        """Build a full key accounting for the context and async_id"""
        key = FuriousAsyncMarker.parent_key(async_id, context_id)
        return ndb.Key(FuriousAsyncMarker, async_id, parent=key)

    @classmethod
    def parent_id(cls, async_id, context_id):
        return str(RING.get_node(async_id + context_id)) + context_id

    @classmethod
    def parent_key(cls, async_id, context_id):
        parent_id = FuriousAsyncMarker.parent_id(async_id, context_id)
        return ndb.Key(FuriousCompletionMarker, parent_id)

    @classmethod
    def from_id(cls, async_id, context_id):
        return cls.full_key(async_id, context_id).get()


class FuriousCompletionMarker(ndb.Model):
    """This entity serves as a 'complete' marker for the entire context."""

    complete = ndb.BooleanProperty(default=False, indexed=False)
    has_errors = ndb.BooleanProperty(default=False, indexed=False)

    @classmethod
    def full_key(cls, id):
        """Build a key for the context so that the context will be in its own
        entity group"""

        key = ndb.Key(FuriousCompletionMarker, id)
        return ndb.Key(FuriousCompletionMarker, id, parent=key)

    @classmethod
    def from_id(cls, id):
        """Helper to pull down the right marker"""

        return FuriousCompletionMarker.full_key(id).get()


class ContextResult(ContextResultBase):

    BATCH_SIZE = 10

    def __init__(self, context):
        self._context = context
        self._task_cache = {}
        self._marker = None

    @property
    def _tasks(self):
        if self._task_cache:
            return ((key, task) for key, task in self._task_cache.iteritems())

        return iter_context_results(self._context, self.BATCH_SIZE,
                                    self._task_cache)

    @property
    def _completion_marker(self):
        if not self._marker:
            self._marker = FuriousCompletionMarker.from_id(self._context.id)

        return self._marker

    def items(self):
        """Yield the async reuslts for the context."""
        for key, task in self._tasks:
            if not (task and task.result):
                yield key, None
            else:
                yield key, json.loads(task.result)["payload"]

    def values(self):
        """Yield the async reuslt values for the context."""
        for _, task in self._tasks:
            if not (task and task.result):
                yield None
            else:
                yield json.loads(task.result)["payload"]

    def has_errors(self):
        """Return the error flag from the completion marker."""
        if self._completion_marker:
            return self._completion_marker.has_errors

        return False


def context_completion_checker(async):
    """Persist async marker and async the completion check"""

    store_async_result(async)

    logging.debug("Check completion for: %s", async.context_id)

    # Check if we are complete
    complete = _query_check(async.context_id)

    if complete:
        logging.debug("Complete Query complete. Running marker check")
        return _completion_checker(async.id, async.context_id)

    # If we were not complete then insert the tail behind
    _insert_completion_checker(async.id, async.context_id, retry=3)

    return True


def _insert_completion_checker(async_id, context_id, retry=None):
    """Wrapper around inserting a task to check completion"""
    from furious.async import Async

    current_queue = _get_current_queue()
    logging.debug("Completion Check queue:%s", current_queue)

    name = context_id

    if retry:
        name = "-".join((context_id, str(retry)))

    Async(_completion_checker, queue=current_queue,
          args=(async_id, context_id, retry),
          task_args={'name': name}).start()


def _get_current_queue():
    """Pull out the queue from the environment"""
    return os.environ.get(QUEUE_HEADER, DEFAULT_QUEUE)


def _query_check(context_id):
    """Use an ancestor query to establish if there is a reason to pull all the
    markers and check for failure"""

    context = None

    try:
        context = get_current_context()
    except errors.NotInContextError:
        context = FuriousContext.from_id(context_id)

    if not context:
        logging.debug("_query_check: Unable to find context %s ", context_id)
        return False

    num_tasks = len(context.task_ids)
    total_found = 0
    async_queries = gen_query_async(context_id)

    start_time = time.time()
    for query in async_queries:
        total_found += len(query.get_result())
        if total_found == num_tasks:
            finished = time.time() - start_time
            logging.debug("_query_check: completed %s %s:%s query time: %s",
                          context_id, num_tasks, total_found, finished)

            return True

    finished = time.time() - start_time
    logging.debug("_query_check: incomplete %s %s:%s query time: %s",
                  context_id, num_tasks, total_found, finished)


def gen_query_async(context_id):
    """Will yield out the  for the given markers"""

    shards = SPOTS[0:]
    shuffle(shards)
    queries = []
    for spot in shards:
        parent_id = str(spot) + context_id
        key = ndb.Key(FuriousCompletionMarker, parent_id)
        queries.append(FuriousAsyncMarker.query(ancestor=key))

    #shuffle(queries)
    for query in queries:
        yield query.fetch_async(keys_only=True)


def _completion_checker(async_id, context_id, retry=None):
    """Check if all Async jobs within a Context have been run."""

    if retry:
        retry = retry - 1
        logging.debug("Tries left %s" % retry)

    if not context_id:
        logging.debug("Context for async %s does not exist", async_id)
        return

    context = FuriousContext.from_id(context_id)
    marker = FuriousCompletionMarker.from_id(context_id)

    if not marker:
        logging.debug("No marker found for context_id %s", context_id)
        if retry and retry > 0:
            _insert_completion_checker(async_id, context_id, retry)
        return False

    if marker and marker.complete:
        logging.debug("Context %s already complete" % context_id)
        return True

    done, has_errors = _check_markers(context_id, context.task_ids)

    if not done:
        if retry and retry > 0:
            _insert_completion_checker(async_id, context_id, retry)
        logging.debug("Context %s incomplete" % context_id)
        return False

    result = _mark_context_complete(marker, context, has_errors)

    if result:
        logging.debug("Context %s complete" % context_id)

    return result


def _check_markers(context_id, task_ids, offset=10):
    """Returns a flag for markers being found for the task_ids. If all task ids
    have markers True will be returned. Otherwise it will return False as soon
    as a None result is hit.
    """
    existing_failure = FuriousContextFailed.check_context_failed(context_id)
    if existing_failure:
        return True, True

    return True, False

    shuffle(task_ids)
    has_errors = False

    for index in xrange(0, len(task_ids), offset):
        keys = [FuriousAsyncMarker.full_key(id, context_id)
                for id in task_ids[index:index + offset]]

        markers = ndb.get_multi(keys)

        if not all(markers):
            logging.debug("Not all Async's complete")
            return False, None

        # Did any of the aync's fail? Check the success property on the
        # AsyncResult.
        has_errors = not all((marker.success for marker in markers))
        if has_errors:
            return True, has_errors

    return True, has_errors


@ndb.transactional
def _mark_context_complete(marker, context, has_errors):
    """Transactionally 'complete' the context."""

    current = None

    if marker:
        current = marker.key.get()

    if not current:
        return False

    if current and current.complete:
        return False

    current.complete = True
    current.has_errors = has_errors
    current.put()

    # Kick off completion tasks.
    _insert_post_complete_tasks(context)

    return True


def _insert_post_complete_tasks(context):
    """Insert the event's asyncs and cleanup tasks."""

    logging.debug("Context %s is complete.", context.id)

    # Async event handlers
    context.exec_event_handler('complete', transactional=True)

    # Insert cleanup tasks
    try:
        # TODO: If tracking results we may not want to auto cleanup and instead
        # wait until the results have been accessed.
        from furious.async import Async
        Async(_cleanup_markers, queue=CLEAN_QUEUE,
              args=[context.id, context.task_ids],
              task_args={'countdown': CLEAN_DELAY}).start()
    except:
        pass


def _cleanup_markers(context_id, task_ids):
    """Delete the FuriousAsyncMarker entities corresponding to ids."""

    logging.debug("Cleanup %d markers for Context %s",
                  len(task_ids), context_id)

    # TODO: Handle exceptions and retries here.
    delete_entities = [FuriousAsyncMarker.full_key(id, context_id)
                       for id in task_ids]
    delete_entities.append(ndb.Key(FuriousCompletionMarker, context_id))

    ndb.delete_multi(delete_entities)

    logging.debug("Markers cleaned.")


def load_context(id):
    """Load a Context object by it's id."""

    return FuriousContext.from_id(id)


def store_context(context):
    """Persist a furious.context.Context object to the datastore by loading it
    into a FuriousContext ndb.Model.
    """

    logging.debug("Attempting to store Context %s.", context.id)

    entity = FuriousContext.from_context(context)

    # TODO: Handle exceptions and retries here.
    context_key = ndb.Key(FuriousCompletionMarker, context.id)
    marker = FuriousCompletionMarker(id=context.id, parent=context_key)
    key, _ = ndb.put_multi((entity, marker))
    logging.debug("Stored Context with key: %s.", key)

    return key


def store_async_result(async):
    """Persist the Async's result to the datastore."""

    logging.debug("Storing result for %s", async.id)

    status = async.result.status if async.result else -1
    result = None

    if async.result:
        result = json.dumps(async.result.to_dict())

    if not async.context_id:
        _save_async_results(async.id, result, status)
        return

    parent_key = FuriousAsyncMarker.parent_key(async.id, async.context_id)
    _save_async_results(async.id, async.context_id, result, status, parent_key)


def _save_async_results(async_id, context_id, result, status, parent_key=None):
    """Will save the marker if there is not an already existing marker"""

    # QUESTION: Do we trust if the marker had a flag result to just trust it?
    marker = FuriousAsyncMarker.from_id(async_id, context_id)

    if marker:
        logging.debug("Marker already exists for %s.", async_id)
        return

    marker = FuriousAsyncMarker(
        id=async_id, result=result, status=status, parent=parent_key)

    fail_marker = None
    if not marker.success:
        existing_failure = FuriousContextFailed.check_context_failed(context_id)
        if not existing_failure:
            fail_marker = FuriousContextFailed.mark_context_failed(context_id)

    if fail_marker:
        ndb.put_multi((marker, fail_marker))
        return

    marker.put()
    logging.debug("Setting Async result %s using marker: %s to status %s.",
                  result, id, status)


def iter_context_results(context, batch_size=10, task_cache=None):
    """Yield out the results found on the markers for the context task ids."""

    for futures in iget_batches(context.id, context.task_ids,
                                batch_size=batch_size):
        for key, future in futures:
            task = future.get_result()

            if task_cache is not None:
                task_cache[key.id()] = task

            yield key.id(), task


def iget_batches(context_id, task_ids, batch_size=10):
    """Yield out a map of the keys and futures in batches of the batch size
    passed in.
    """
    make_key = lambda _id: FuriousAsyncMarker.full_key(_id, context_id)
    for keys in i_batch(imap(make_key, task_ids), batch_size):
        yield izip(keys, ndb.get_multi_async(keys))


def i_batch(items, size):
    """Generator that iteratively batches items to a max size and consumes the
    items as each batch is yielded.
    """
    for items_batch in iter(lambda: tuple(islice(items, size)),
                            tuple()):
        yield items_batch


def get_context_result(context):
    return ContextResult(context)
