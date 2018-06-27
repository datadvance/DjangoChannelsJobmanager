#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""Simple example of the JobManager usage.

How to run:
```bash
./manage.py migrate
./manage.py runserver
./manage.py runworker example-jobs
```

Start/cancel some jobs:
```bash
for i in `seq 10`; do curl http://127.0.0.1:8080/start_job/; done
for i in `seq 10`; do curl http://127.0.0.1:8080/cancel_job/; done
```
"""

import asyncio
import collections

import channels.routing
import django.http
import django.urls

import channels_jobmanager


class ExampleJobs(channels_jobmanager.JobManager):
    """Example job manager class."""

    # Explicitly set channel name. Run worker to serve this channel:
    # `./manage.py runworker example-jobs`
    job_channel_name = 'example-jobs'

    # Limit the number of jobs per worker.
    jobs_per_worker = 2

    # Customize job when it is submitted.
    @classmethod
    def on_job_submit(cls, job):
        print(f'JOB SUBMITTED:\n{repr(job)}')
        # Add `job_id` argument to the job function.
        job.kwds['job_id'] = job.id
        return job

    # Watch over job state changes.
    @classmethod
    def on_job_update(cls, job):
        print(f'JOB UPDATES:\n{repr(job)}')


# Hack to track the submitted jobs. Never do this in production!
job_ids = collections.deque()


@ExampleJobs.job()
async def long_job(index, count, job_id):
    """Long-running job example."""
    for i in range(count):
        print(f'JOB #{index} ({job_id}) IS WORKING #{i}')
        # Report progress.
        yield {
            'message': 'Doing the job...',
            'payload': {'iteration': i},
            'readiness': (i+1) / count
        }
        await asyncio.sleep(delay=1)

    # Report result.
    yield {
        'message': 'Job well done!',
        'payload': {'steps done': count},
        'readiness': 1.0
    }


def start_job(request):  # pylint: disable=unused-argument
    """Handler for `start_job/` request."""
    job = long_job(index=len(job_ids), # pylint: disable=no-value-for-parameter
                   count=7)
    print('JOB STARTED:', job)
    job_ids.append(job.id)  # pylint: disable=no-member
    return django.http.HttpResponse()


def cancel_job(request):  # pylint: disable=unused-argument
    """Handler for `cancel_job/` request."""
    if not job_ids:
        print('No jobs are running, nothing to cancel!')
    else:
        job_id = job_ids.popleft()
        print('CANCELING JOB:', job_id)
        long_job.cancel(job_id)
    return django.http.HttpResponse()


# Main application referenced in the `settings.py`.
# (This typically done in `routing.py`.)
application = channels.routing.ProtocolTypeRouter({
    'channel': channels.routing.ChannelNameRouter({
        ExampleJobs.job_channel_name: ExampleJobs
    })
})


# Setup URL configuration.
# (This typically done in `urls.py`.)
urlpatterns = [
    django.urls.path('start_job/', start_job),
    django.urls.path('cancel_job/', cancel_job),
]
