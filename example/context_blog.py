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

"""Example of using Context events for work flow.

This example creates a context with events, then adds a few Async jobs to the
Context. This is to illustrate the differences between a serial process and
an async process.

db models were chosen to avoid caching related timing influences
"""


import logging
from datetime import datetime

from furious import context
from furious.async import Async

import webapp2
from google.appengine.ext import ndb


class BlogCompletionHandler(webapp2.RequestHandler):
    """Demonstrate using Context Events to make work flows."""
    def get(self):

        count = self.request.get('tasks', 5)

        start_happy(int(count))

        logging.info('Async jobs for context batch inserted.')

        self.response.out.write(
            'Successfully inserted a test group of %s jobs.' % count)


class Section(ndb.Model):

    happy = ndb.BooleanProperty(indexed=False, default=False)


class HappyMap(ndb.Model):

    data = ndb.JsonProperty(indexed=False)
    start = ndb.DateTimeProperty(auto_now_add=True)
    end = ndb.DateTimeProperty(auto_now=True)

    def update_section(self, section_id, section_happy):

        self.data[section_id] = section_happy


def start_happy(num_sections):
    """Kick off a serial process after the asyncs have been inserted.
    This is to show simple timing differences.
    """

    serial_section_keys = []
    async_section_keys = []

    for num in xrange(num_sections):

        serial_section = Section()
        async_section = Section()

        # Serial insertions, for simplicty of code, not performance
        ndb.put_multi((serial_section, async_section))
        async_section_keys.append(async_section.key)
        serial_section_keys.append(serial_section.key)

    async_map = HappyMap()
    serial_map = HappyMap()
    ndb.put_multi((serial_map, async_map))

    # this will cause some delay for the serial because the asyncs need
    # to be inserted
    make_stuff_happy_async(async_map.key, async_section_keys)
    make_stuff_happy(serial_map.key, serial_section_keys)


def make_stuff_happy_async(happy_map_key, section_keys):

    # To make the timing more 'accurate'
    happy_map = happy_map_key.get()
    happy_map.start = datetime.utcnow()
    happy_map.put()

    happy_map_key_id = happy_map_key.id()

    with context.new(persist_async_results=True) as ctx:
        for key in section_keys:
            async = Async(make_happy, args=[key.urlsafe()])
            ctx.add(async)

        completion_async = Async(_make_happy_complete, args=[happy_map_key_id])

        ctx.set_event_handler('complete', completion_async)


def _make_happy_complete(happy_map_key_id):

    from furious.context import get_current_async_with_context

    _, context = get_current_async_with_context()

    if not context:
        return

    happy_documents = []
    happy_sections = []

    for async_id, async_result in context.result.items():
        if not async_result:
            continue

        key = ndb.Key(urlsafe=async_result)
        happy_sections.append(key.id())

    if happy_sections:
        happy_map_key = ndb.Key(urlsafe=happy_map_key_id)
        update_happy_map(happy_map_key, happy_sections, happy_documents)


def make_stuff_happy(happy_map_key, section_keys):

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
        update_happy_map(
            happy_map_key, happy_section_ids)

    happy_map = happy_map_key.get()
    running_time = happy_map.end - happy_map.start
    logging.info("Serial took %s seconds" % (running_time.total_seconds()))


@ndb.transactional
def make_happy(entity_key):

    # Just to keep things DRY between the async and the serial versions
    if isinstance(entity_key, unicode):
        entity_key = ndb.Key(urlsafe=entity_key)

    current_entity = entity_key.get()

    if not current_entity:
        return None

    current_entity.happy = True

    current_entity.put()

    return entity_key


@ndb.transactional
def update_happy_map(happy_map_key, happy_section_ids):

    current_map = happy_map_key.get()

    for happy_section in happy_section_ids:
        current_map.update_section(happy_section, True)

    current_map.put()
