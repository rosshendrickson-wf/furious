from collections import deque
from itertools import imap
from itertoosl import islice
from random import shuffle

from google.appengine.ext import ndb
from furious.context import get_current_context

from furious.async import Async


class FuriousMarker(ndb.Model):

    options = ndb.JSONproperty(indexed=False)
    failed = ndb.BooleanProperty(indexed=False, default=False)


class FuriousResult(ndb.Model):

    payload = ndb.JSONproperty()


def store_async_result(async_id, result):
    """Persist the Async's result to the datastore."""
    # TODO: Need to handle if we run into exceptions here
    FuriousResult(id=async_id, payload=result).put()


def store_async_marker(async):
    """Persist a marker indicating the Async ran in the datastore."""
    if not async:
        return

    FuriousMarker(id=async.id).put()
    Async(target=completion_check).start()


def load_async_result(async_id):

    result = FuriousResult.get_by_id(async_id)
    if result:
        return result.options


def load_context(context_id):
    """Loads a marker containing the JSON of the stored Contex. Returns a dict
    """
    result = FuriousMarker.get_by_id(context_id)
    if result:
        return result.options


def store_context(context_id, context_options):
    """Stores a marker that stores the context as a dictonary."""

    marker = FuriousMarker(id=context_id, options=context_options)
    markers = [marker]
    on_complete = context_options.pop('on_complete', {})

    if on_complete:

        completion = CompletionMarker(id=context_id)
        completion.on_complete = on_complete
        task_ids = context_options.pop('_task_ids', [])
        completion.async_state = {task_id: INCOMPLETE for task_id in task_ids}
        markers.append(completion)

    ndb.put_multi(markers)

INCOMPLETE = 0
COMPLETE = 1
FAILED = -1


class CompletionMarker(ndb.Model):

    complete = ndb.BooleanProperty(indexed=False, default=False)
    async_state = ndb.JSONProperty(indexed=False)
    on_complete = ndb.JSONProperty(indexed=False)


@ndb.tasklet()
def build_marker_update(marker_id):
    """Tasklet that will return an updated dictionary with an async's state"""

    result = {marker_id: INCOMPLETE}
    marker = yield FuriousMarker.get_by_id_async(marker_id)

    if not marker:
        raise ndb.Return(result)

    state = COMPLETE
    if marker.failed:
        state = FAILED

    result = {marker_id: state}
    raise ndb.Return(result)


def _incomplete_asyncs(marker):
    """Simple helper to check if there are any incompletes in the state. """
    return [marker_id for marker_id, value in marker.async_state
            if value is INCOMPLETE]


def deque_take(n, iterable):
    """Will return a deque of islice of an iterable."""
    return deque(islice(iterable, n))


@ndb.transactional
def _complete_marker(marker):
    """Transactionaly ensures we only call run callbacks once."""

    first_complete = False
    current = marker.key.get()

    if current.complete:
        return first_complete, current

    marker.complete = True
    first_complete = True
    marker.put()

    return first_complete, marker


def completion_check():
    """Should be run after each Async persists."""

    context = get_current_context()
    completion_marker = CompletionMarker.get_by_id(context.id)

    if not completion_marker:
        return

    if completion_marker.complete:
        return

    remaining_ids = _incomplete_asyncs(completion_marker)

    # randomize which markers we get first to avoid 'hot' markers
    shuffle(remaining_ids)

    # map to a tasklet to concurrently get the remaining task updates
    updates = imap(build_marker_update, remaining_ids)

    incomplete = apply_updates(updates, completion_marker)

    if incomplete:
        return

    # We are complete
    first_complete = False
    first_complete, completion_marker = _complete_marker(
        completion_marker)

    if first_complete:
        run_callbacks(completion_marker)


def apply_updates(updates, marker):
    """Will continue checking markers until it either finds an incomplete or
    finishes going through all of the updates."""

    incomplete = False
    # TODO Determine a good batch size
    batch_size = 100
    while True:

        if incomplete:
            break

        # A deque is more memory efficient than a list and will help with
        # memory fragmentation on an instance
        group = deque_take(batch_size, updates)

        if not group:
            break

        for update in group:

            result = update.get_result()

            if INCOMPLETE in result.itervalues():
                incomplete = True
                break

            marker.async_state.update(result)

    return incomplete


def run_callbacks(marker):
    """Unpack from the payload the callback and start it."""
    # for the moment we just assume we have one
    on_complete = marker.on_complete.get('on_complete')

    if on_complete:
        Async.from_dict(on_complete).start()
