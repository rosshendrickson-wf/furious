import logging

from kepler.api import mark_work_complete
from kepler.api import setup_completion_callback
from kepler.api import add_work_to_work_id

from .async import Async
from .context.context import Context
from .context import get_current_context
from .context import get_current_async
from .job_utils import path_to_reference

from .job_utils import decode_callbacks
from .job_utils import encode_callbacks
from .job_utils import get_function_path_and_options
from .job_utils import reference_to_path

from . import errors


class CompleteMixin(object):

    @property
    def completion_id(self):

        return self._options.get('completion_id', None)

    @completion_id.setter
    def completion_id(self, completion_id):

        self._options['completion_id'] = completion_id

    @property
    def on_success(self):

        callbacks = self._options.get('callbacks')

        if callbacks:
            return callbacks.get('on_success')

    @on_success.setter
    def on_success(self, on_success):

        callbacks = self._options.get('callbacks', {})

        if not isinstance(on_success, (Async, Context)):
            on_success = reference_to_path(on_success)

        callbacks['on_success'] = on_success

        self._options['callbacks'] = callbacks

    @property
    def on_failure(self):
        callbacks = self._options.get('callbacks')

        if callbacks:
            return callbacks.get('on_failure')

    @on_failure.setter
    def on_failure(self, on_failure):

        callbacks = self._options.get('callbacks', {})

        if not isinstance(on_failure, (Async, Context)):
            on_failure = reference_to_path(on_failure)

        callbacks['on_failure'] = on_failure

        self._options['callbacks'] = callbacks

    @property
    def process_results(self):

        process = self._options['_process_results']
        if not process:
            return None

        if not callable(process):
            return path_to_reference(process)

        return process

    @process_results.setter
    def process_results(self, process):

        if callable(process):
            process = reference_to_path(process)

        self._options['_process_results'] = process


def initialize_completion(node):
    """
    """

    work_ids = [node.id]
    args = _gen_callback_args(node)

    completion_id = setup_completion_callback(
        work_ids=work_ids,
        on_success=completion_callback,
        callback_args=args,
        prefix="FURIOUS",
        entries_per_entity=20)

    node.completion_id = completion_id

    if isinstance(node, Async):
        node.process_results = _process_completion_result

    if isinstance(node, Context):
        add_context_work(completion_id, node, node._tasks, args)

    return completion_id


def add_context_work(completion_id, parent, asyncs, args=None):

    if len(asyncs) < 1:
        return

    work_ids = []

    for async in asyncs:
        async.completion_id = completion_id
        async.process_results = _process_completion_result
        work_ids.append(async.id)

    if not args:
        args = _gen_callback_args(parent)

    add_work_to_work_id(completion_id, parent.id, work_ids,
                        on_success=completion_callback,
                        on_success_args=args)


def _gen_callback_args(node):

    callbacks = node._options.get('callbacks')
    args = {}
    if callbacks:
        args['callbacks'] = encode_callbacks(callbacks)

    return args


def handle_completion_start(node):

    callbacks = node._options.get('callbacks')

    if not callbacks:
        return

    if not node.on_success or not node.on_failure:
        return

    # If we are in a context with a completion id then we need to add to it
    current_context = None
    try:

        current_context = get_current_context()

    except errors.NotInContextError:
        logging.debug('no context')

    # not in a context or an async
    # no completion graph
    if not current_context or not current_context.completion_id:
        initialize_completion(node)
        return

    # we assume that we are in a context or part of an async
    if isinstance(node, Context):
        add_context_work(current_context.completion_id, node, node._tasks)
    else:
        add_context_work(
            current_context.completion_id, current_context, [node])


def _process_completion_result():
    from furious.processors import AsyncException

    async = get_current_async()

    if isinstance(async.result, AsyncException):
        mark_async_complete(async, True, async.result)

    mark_async_complete(async)


def mark_async_complete(async, failed=False, failed_payload=None):

    mark_work_complete(async.completion_id, async.id, failed, failed_payload)


def execute_completion_callbacks(callbacks, failed=False, failed_kwargs=None):

    if not callbacks:
        return

    callbacks = decode_callbacks(callbacks)
    callback = None

    if failed:
        callback = callbacks.get('on_failure')
    else:
        callback = callbacks.get('on_success')

    _execute_callback(callback)


def _execute_callback(callback):

    if not callback:
        print 'no callback'
        return

    print 'callback', callback

    if isinstance(callback, (Context, Async)):
        callback.start()

    if callable(callback):
        function, options = get_function_path_and_options(callback)

        Async(target=function, options=options).start()


completion_callback = reference_to_path(execute_completion_callbacks)
