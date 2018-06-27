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

"""The database model for the job."""

import uuid

from django.contrib.auth.models import User
from django.db import models
import jsonfield


class JobBase(models.Model):
    """Abstract job model class.

    To customize a job model subclass this class and add all the fields
    you need. To enable custom job model set `job_model_class` in the
    `JobManager` subclass. By default `JobManager` uses `Job` as job
    model.
    """

    class Meta:
        abstract = True

    # All possible job stats.
    STATES = (
        ('PENDING', 'Pending'),
        ('WORKING', 'Working'),
        ('CANCELING', 'Canceling'),
        ('DONE', 'Done'),
        ('ERROR', 'Error'),
        ('CANCELED', 'Canceled'),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)

    # User owning the job.
    user = models.ForeignKey(User, null=True, blank=True,
                             on_delete=models.CASCADE)

    # Flag, indicating that job can be canceled. By default all jobs can
    # be canceled - any job can be dropped from `PENDING` to `CANCELED`
    # directly until it is started.
    is_cancelable = models.BooleanField(default=True)

    # Job state.
    state = models.CharField(choices=STATES, max_length=16, default='PENDING')

    # -------------------------------------------------------------- TIMESTAMPS

    # Job submission time.
    submitted = models.DateTimeField(auto_now_add=True)

    # Time when job started.
    started = models.DateTimeField(null=True, default=None)

    # Time when job updated its status for a last time, if status is
    # FINISHED or FAILED then this is the time of completion.
    updated = models.DateTimeField(auto_now=True)

    # --------------------------------------------------- JOB EXECUTION DETAILS

    # Tha channel name job manager performs on. In essence this field
    # identifies the job manager current job belongs to.
    channel_name = models.CharField(max_length=128)

    # Job function name. Used to find proper callable inside appropriate
    # job manager class in the registry.
    function_name = models.CharField('job function name', max_length=128)

    # Positional arguments of the job function. Value is a
    # JSON-formatted string containing a list of values.
    args = jsonfield.JSONField('job function args', default=list())

    # Keyword arguments of the job function. Value is a
    # JSON-formatted string containing a list of values.
    kwds = jsonfield.JSONField('job function kwds', default=dict())

    # -------------------------------------------------------------- JOB STATUS

    # The data in this section is updated automatically based on the
    # values yielded from the job functions.

    # User message describing current job status.
    message = models.TextField(default='')

    # JSON-formatted string containing details about current job status.
    # The information stored here is job-specific and does not processed
    # by the job execution routines. This field is used for both:
    # intermediate progress reports and final job execution result.
    payload = jsonfield.JSONField(default=dict())

    # A readiness of the job: floating point number from 0 to 1. Can
    # have value `NaN` which indicates that it is impossible to
    # calculate readiness.
    readiness = models.FloatField(null=True, default=None)

    def __repr__(self):
        """String job representation useful for debugging."""

        result = f'<{self.__class__.__name__}\n'
        for field in self._meta.get_fields():
            if not field.concrete:
                continue
            result += f'\t{field.name}: {getattr(self, field.name)}\n'
        result += '>'
        return result


class Job(JobBase):
    """Default job model."""
    pass
