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


"""Jobs service Django signals.

Here we subscribe to modifications of the Job model.
"""
import django.db.models.signals
import django.db.transaction
import django.dispatch

from .jobmanager import JobManager
from .models import Job


@django.dispatch.receiver(django.db.models.signals.post_save, sender=Job)
def job_updated(sender, **kwds):  # pylint: disable=unused-argument
    """Let JobManager know that some Job object changes."""

    # The method is called each time `save()` of some job is called, but
    # we do not want to send notifications (call
    # `JobManager.on_job_update`) for modifications that can be rolled
    # back. So if we are in a transaction then notify proper job manager
    # only when transaction successfully commits, if we are not in a
    # transaction then this will invoke given lambda immediately.
    #
    # The only drawback of submitting this `on_commit` notification here
    # in the `post_save` handler is if there are multiple invocations of
    # `save()` inside a single transaction then we submit such
    # notification multiple times. Since in the `JobManager`
    # implementation it is not the case we are OK with such approach.
    django.db.transaction.on_commit(
        # pylint: disable=protected-access
        lambda: JobManager._send_job_update_notification(kwds['instance'])
    )
