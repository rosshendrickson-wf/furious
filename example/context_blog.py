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

"""
This example was used in a blog post to illustrate the differences between a
serial process and an async process and the advantages of using a context and
its completion callback function instead of a serial process that does a series
of work and then a final function over all the things that were iterated over

results = []
for n in ns:
    result = do_thing(n)
    results.append(result)

update_a(results)

where do_thing and update_for_ns are doing transactional writes on datastore
entities.

The ExampleSection ndb model has its caching disabled to simulate working with
db or cold entities.

If you run this on an appspot you can piece the performance data together by
searching for the _make_happy_complete call and the start_happy call. Both
will log out information about timing. If you run this on the dev appserver
the async will always be slower.
"""


import logging
from datetime import datetime

from furious import context
from furious.async import Async
from furious.context import get_current_async_with_context

import webapp2
from google.appengine.ext import ndb


class BlogCompletionHandler(webapp2.RequestHandler):
    """Demonstrate Differences between serial and async processes.
    """
    def get(self):

        count = self.request.get('tasks', 5)

        Async(start_happy, args=[int(count)]).start()

        logging.info('Async jobs for context batch inserted.')

        self.response.out.write(
            'Successfully started two test groups of %s sections.' % count)


class ExampleSection(ndb.Model):

    happy = ndb.BooleanProperty(indexed=False, default=False)
    _use_cache = False
    _use_memcache = False


class HappyMap(ndb.Model):

    data = ndb.JsonProperty(indexed=False)
    start = ndb.DateTimeProperty(auto_now_add=True)
    end = ndb.DateTimeProperty(auto_now=True)

    def update_section(self, section_id, section_happy):
        """Update an entry in the entity's data map.
        """

        if not self.data:
            self.data = {}

        self.data[section_id] = section_happy


def start_happy(num_sections):
    """Kick off a serial process after the asyncs have been inserted.
    This is to show simple timing differences.
    """

    serial_section_keys = []
    async_section_keys = []

    for num in xrange(num_sections):

        serial_section = ExampleSection()
        async_section = ExampleSection()

        # Serial insertions, for simplicty of code, not performance
        ndb.put_multi((serial_section, async_section))
        async_section_keys.append(async_section.key)
        serial_section_keys.append(serial_section.key)

    async_map = HappyMap()
    serial_map = HappyMap()
    ndb.put_multi((serial_map, async_map))

    # this will cause some delay for the serial because the asyncs need
    # to be inserted but we restart the start time at the beginning of the
    # serial call
    make_stuff_happy_async(async_map.key, async_section_keys,
                           serial_map.key.urlsafe())
    make_stuff_happy(serial_map.key, serial_section_keys)

    logging.info("Async map key %s" % async_map.key.id())

    # Clean up our test entities from the datastore - 20 minutes later
    Async(_cleanup_test, args=[serial_map.key.urlsafe(),
                               async_map.key.urlsafe()],
          task_args={'countdown': 1200}).start()


def make_stuff_happy_async(happy_map_key, section_keys, serial_id=None):
    """Async off the updates to each section and have a completion callback
    that updates the map.
    """

    # To make the timing more 'accurate'
    happy_map = happy_map_key.get()
    happy_map.start = datetime.utcnow()
    happy_map.put()

    happy_map_key_id = happy_map_key.urlsafe()

    with context.new(persist_async_results=True) as ctx:
        for key in section_keys:
            async = Async(make_happy, args=[key.urlsafe()])
            ctx.add(async)

        completion_async = Async(_make_happy_complete, args=[happy_map_key_id,
                                                             serial_id])

        ctx.set_event_handler('complete', completion_async)


def _make_happy_complete(async_map_key_id, serial_map_key_id):
    """Simple Completion handler that verifies what work was done during the
    fan out phase and then updates a map that is used elsewhere."""

    _, context = get_current_async_with_context()

    if not context:
        return

    happy_sections = []

    for async_id, async_result in context.result.items():
        if not async_result:
            continue

        key = ndb.Key(urlsafe=async_result)
        happy_sections.append(key.id())

    if happy_sections:
        happy_map_key = ndb.Key(urlsafe=async_map_key_id)
        update_happy_map(happy_map_key, happy_sections)

    logging.info("Context Complete %s %s" % (context, context.result))

    # Testing related
    happy_map = happy_map_key.get()
    running_time = happy_map.end - happy_map.start
    logging.info("Total number of sections %s" % len(happy_map.data))
    logging.info("Async %s took %s seconds" % (
        happy_map_key.id(), running_time.total_seconds()))

    happy_map_key = ndb.Key(urlsafe=serial_map_key_id)
    happy_map = happy_map_key.get()
    running_time = happy_map.end - happy_map.start
    logging.info("Serial %s took %s seconds" % (
        happy_map_key.id(), running_time.total_seconds()))


def make_stuff_happy(happy_map_key, section_keys):
    """Serially transactionally update each section.
    """
    # To make the timing more 'accurate'
    happy_map = happy_map_key.get()
    happy_map.start = datetime.utcnow()
    happy_map.put()

    happy_section_ids = []

    for section_key in section_keys:
        result = make_happy(section_key)
        if result:
            happy_section_ids.append(section_key.id())

    if happy_section_ids:
        update_happy_map(happy_map_key, happy_section_ids)

    # Testing related
    happy_map = happy_map_key.get()
    running_time = happy_map.end - happy_map.start
    logging.info("Serial %s took %s seconds" % (
        happy_map_key.id(), running_time.total_seconds()))


@ndb.transactional
def make_happy(entity_key):
    """Update a single property on an entity in a transaction.
    """

    # Just to keep things DRY between the async and the serial versions
    if isinstance(entity_key, unicode):
        entity_key = ndb.Key(urlsafe=entity_key)

    current_entity = entity_key.get()

    if not current_entity:
        return None

    current_entity.happy = True

    current_entity.put()

    return entity_key.urlsafe()


@ndb.transactional
def update_happy_map(happy_map_key, happy_section_ids):
    """Transactionally update the Happy Map
    """
    current_map = happy_map_key.get()

    if not current_map:
        logging.info("No map found. Aborting")
        return

    for happy_section in happy_section_ids:
        current_map.update_section(happy_section, True)

    current_map.put()


def _cleanup_test(serial_urlsafe, async_urlsafe):
    """Clean up our entities used running this example.
    """

    serial_map_key = ndb.Key(urlsafe=serial_urlsafe)
    async_map_key = ndb.Key(urlsafe=serial_urlsafe)

    entity_keys = [serial_map_key, async_map_key]
    serial_map, async_map = ndb.get_multi(entity_keys)

    serial_keys = [ndb.Key(ExampleSection, section_id)
                   for section_id in serial_map.data.iterkeys()]

    async_keys = [ndb.Key(ExampleSection, section_id)
                  for section_id in async_map.data.iterkeys()]

    entity_keys.extend(serial_keys)
    entity_keys.extend(async_keys)

    ndb.delete_multi(entity_keys)

    logging.info("Deleted %s example entities" % len(entity_keys))
