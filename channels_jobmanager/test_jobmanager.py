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

"""Tests for the JobManager."""

import asyncio
import types
import uuid

import channels.db
import channels.layers as ch_layers
import channels.testing as ch_testing
import django.db.utils
import pytest

from channels_jobmanager import jobmanager


# Disable warning that outer name is redefined: `pytest` dependency
# injection works this way. We also OK with the fact that this file is
# quite large.
# pylint: disable=redefined-outer-name, too-many-lines


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_done(my_job):
    """Test simple uncancelable job function that finishes OK.

    Features:
      - Test asserts that simple job is reported as cancelable only when
        it is in the `PENDING` or `CANCELING` state.
      - Test makes sure job has proper status when finishes and job
        update handler is invoked during the job lifetime correct number
        of times.
    """

    # Set up callback to get notifications when job state changes.
    job = None
    job_update_counter = 0

    def on_job_update(_job):
        """The callback updates `job` and `job_update_counter`."""
        nonlocal job, job_update_counter
        job = _job
        job_update_counter += 1
        # Assert that simple job is reported as cancelable only when it
        # is in the `PENDING` or `CANCELING` state.
        if job.state in ['PENDING', 'CANCELING']:
            assert job.is_cancelable, (  # pylint: disable=no-member
                'Job is not cancelable when it must be cancelable!')
        else:
            assert not job.is_cancelable, (  # pylint: disable=no-member
                'Job is cancelable when it must not be cancelable!')

    my_job.set_on_update(on_job_update)

    # Submit a job which must finish OK.
    new_job = await my_job.job(mustfail=False)

    # Check job instance of new job (returned from job function).
    assert new_job.is_cancelable, (  # pylint: disable=no-member
        'Job instance states that just submitted job is not cancelable!')
    assert new_job.state == 'PENDING'
    assert new_job.id is not None
    assert new_job.started is None
    assert new_job.args == []
    assert new_job.kwds == {'mustfail': False}
    assert new_job.message == ''
    assert new_job.payload == {}
    assert new_job.readiness is None

    # Check current job state right after job is submitted.
    assert job.state == 'PENDING', ('Just submitted job has wrong state '
                                    f'`{job.state}`!')

    # Process ASGI messages and wait for the job to finish.
    await my_job.process_jobs()

    # Check job state when job is done.
    assert job.state == 'DONE', f'Finished job has wrong state `{job.state}`!'

    # Check that job update callback has been called three times:
    #   1. job is submitted
    #   2. job switches to the working state
    #   3. job finishes
    assert job_update_counter == 3, 'Job updated wrong number of times!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_failed(my_job):
    """Test simple uncancelable job which fails."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job

    my_job.set_on_update(on_job_update)

    # Submit a job which must fail.
    await my_job.job(mustfail=True)

    # Process ASGI messages and wait for the job to finish.
    await my_job.process_jobs()

    # Check a state of the job.
    assert job.state == 'ERROR', f'Failed job has wrong state `{job.state}`!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_discarded(my_job):
    """Test uncancelable job can be canceled if it is not started."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job
        # Asserts that job is either pending or canceled.
        assert job.state in ['PENDING', 'CANCELED'], (
            'Job that canceled immediately after submission has wrong '
            f'state `{job.state}`!')

    my_job.set_on_update(on_job_update)

    # Submit a job.
    new_job = await my_job.job(mustfail=False)

    # It must be canceled OK, because we are sure it has not started.
    my_job.job_manager_class.cancel(new_job.id)

    # Process ASGI messages but do not wait for jobs (no jobs started).
    await my_job.process_jobs()

    # Check a state of the job.
    assert job.state == 'CANCELED', ('Canceled job has wrong state '
                                     f'`{job.state}`!')


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_canceled(my_job):
    """Test that uncancelable job cannot be canceled when working."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job

        if job.state in ['WORKING', 'DONE', 'ERROR']:
            canceled = my_job.job_manager_class.cancel(job.id)
            assert not canceled, (
                f'Uncancelable job is canceled in the `{job.state}` state!')

    my_job.set_on_update(on_job_update)

    # Submit a job that fails.
    await my_job.job(mustfail=True)

    # Process ASGI messages and wait for the job to finish.
    await my_job.process_jobs()

    # Check a state of the job.
    assert job.state == 'ERROR', f'Failed job has wrong state `{job.state}`!'

    # Submit a job that succeeds.
    await my_job.job(mustfail=False)

    # Process ASGI messages and wait for the job to finish.
    await my_job.process_jobs()

    # Check a state of the job.
    assert job.state == 'DONE', f'Finished job has wrong state `{job.state}`!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_gen_done(my_job_gen):
    """Test simple cancelable job (yields the progress) finishes OK.

    Features:
      - Make sure job state tells the job is cancelable when job is in
        `PENDING`, `WORKING`, and `CANCELING` state and is not
        cancelable in other states.
      - Check the number of times job update handler called.
      - Check job state before and after its execution.
    """

    # Set up callback to get notifications when job state changes.
    job = None
    job_update_counter = 0

    def on_job_update(_job):
        """The callback updates `job` and `job_update_counter`."""
        nonlocal job, job_update_counter
        job = _job
        job_update_counter += 1
        # Make sure job state tells the job is cancelable when job is in
        # `PENDING`, `WORKING`, and `CANCELING` state and is not
        # cancelable in other states.
        if job.state in ['PENDING', 'WORKING', 'CANCELING']:
            assert job.is_cancelable, ('Job is not cancelable when it '
                                       'must be cancelable!')
        else:
            assert not job.is_cancelable, ('Job is cancelable when it '
                                           'must not be cancelable!')

    my_job_gen.set_on_update(on_job_update)

    # Submit a job which must finish OK.
    new_job = await my_job_gen.job(yieldsteps=1, mustfail=False)

    # Check that job is cancelable.
    assert new_job.is_cancelable, ('Job instance states that cancelable job'
                                   'is not cancelable!')

    # Check job state right after job is submitted.
    assert job.state == 'PENDING', ('Submitted job has wrong state '
                                    f'{job.state}!')

    # Process ASGI messages and wait for the job to finish.
    await my_job_gen.process_jobs()

    # Check job state when job is done.
    assert job.state == 'DONE', f'Finished job has wrong state `{job.state}`!'

    # Check that job update callback has been called four times:
    #   1. job is submitted
    #   2. job switches to the working state
    #   3. job reports the progress
    #   4. job finishes
    assert job_update_counter == 4, 'Job updated wrong number of times!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_gen_failed(my_job_gen):
    """Test simple cancelable job (yields the progress) that fails."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job

    my_job_gen.set_on_update(on_job_update)

    # Submit a job which must fail.
    await my_job_gen.job(yieldsteps=1, mustfail=True)

    # Process ASGI messages and wait for the job to finish.
    await my_job_gen.process_jobs()

    # Check job state when job is done.
    assert job.state == 'ERROR', f'Failed job has wrong state `{job.state}`!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_gen_discarded(my_job_gen):
    """Check that job will not start if canceled in advance."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job
        # Asserts that job is either pending or canceled.
        assert job.state in ['PENDING', 'CANCELED'], (
            'Job that canceled immediately after submission has wrong '
            'state `{job.state}`!'
        )

    my_job_gen.set_on_update(on_job_update)

    # Submit a job which must fail immediately.
    new_job = await my_job_gen.job(yieldsteps=0, mustfail=True)

    # Cancel the job before it has chance to start.
    my_job_gen.job_manager_class.cancel(new_job.id)

    # Process ASGI messages but do not wait for jobs (no jobs started).
    await my_job_gen.process_jobs()

    # Check a state of the job.
    assert job.state == 'CANCELED', ('Canceled job has wrong state '
                                     f'`{job.state}`!')


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_gen_canceled(my_job_gen):
    """Check the job can be canceled in the middle of execution.

    In this test we need to be sure that we cancel already running job
    (i.e. in the `WORKING` state). This is important because job which
    is not started simply changes its state from `PENDING` to `CANCELED`
    directly (and that case is checked in other tests). Here we test
    that job can (or cannot) be canceled in the middle of execution.
    """

    # Setup callback which is invoked when job state changes.
    job = None
    job_update_counter = 0

    def on_job_update(_job):
        """Job update handler to cancel job after it is started."""
        nonlocal job, job_update_counter

        # Cancel the job when it updates in the `WORKING` state for the
        # second time. We do it just to be sure it is somewhere in the
        # middle of executions.
        if (job is not None and
                _job.state == job.state == 'WORKING'):
            my_job_gen.job_manager_class.cancel(job.id)

        job = _job
        job_update_counter += 1

    my_job_gen.set_on_update(on_job_update)

    # Submit a job fails and tell it to do multiple iterations.
    await my_job_gen.job(yieldsteps=42, mustfail=True)

    # Process ASGI messages and wait for the job to finish.
    await my_job_gen.process_jobs()

    # Check a state of the job.
    assert job.state == 'CANCELED', ('Canceled job has wrong state '
                                     f'`{job.state}`!')

    # Check number of updates:
    # PENDING -> WORKING (x2) -> CANCELING (x2) -> CANCELED
    assert job_update_counter == 6, 'Incorrect number of job updates detected!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_async_done(my_job_async):
    """Test job specified by async function which finishes OK.

    Features:
      - Test asserts that the job is reported as cancelable only when it is
        in the `PENDING` or `CANCELING` state.
      - Test makes sure job has proper status when finishes and job update
        handler is invoked during the job lifetime correct number of times.
    """

    # Set up callback to get notifications when job state changes.
    job = None
    job_update_counter = 0

    def on_job_update(_job):
        """The callback updates `job` and `job_update_counter`."""
        nonlocal job, job_update_counter
        job = _job
        job_update_counter += 1
        # Assert that the job is reported as cancelable only when it is
        # in the `PENDING` or `CANCELING` state.
        if job.state in ['PENDING', 'CANCELING']:
            assert job.is_cancelable, ('Job is not cancelable when it '
                                       'must be cancelable!')
        else:
            assert not job.is_cancelable, ('Job is cancelable when it '
                                           'must not be cancelable!')

    my_job_async.set_on_update(on_job_update)

    # Submit a job which must finish OK.
    new_job = await my_job_async.job(mustfail=False)

    # Check job is cancelable until it is started.
    assert new_job.is_cancelable, ('Job instance states that just submitter '
                                   'job is not cancelable!')

    # Process ASGI messages and wait for the job to finish.
    await my_job_async.process_jobs()

    # Check job state when job is done.
    assert job.state == 'DONE', f'Finished job has wrong state `{job.state}`!'

    assert not job.is_cancelable, ('Job instance states that uncancelable '
                                   'job can be canceled!')

    # Check that job update callback has been called three times:
    #   1. job is submitted
    #   2. job switches to the working state
    #   3. job finishes
    assert job_update_counter == 3, 'Incorrect number of job updates detected!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_async_failed(my_job_async):
    """Test job specified by async function which fails.

    Test submits job which fails and check its state at the end.
    """

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job

    my_job_async.set_on_update(on_job_update)

    # Submit a job which must fail.
    await my_job_async.job(mustfail=True)

    # Process ASGI messages and wait for the job to finish.
    await my_job_async.process_jobs()

    # Check a state of the job.
    assert job.state == 'ERROR', f'Failed job has wrong state `{job.state}`!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_async_discarded(my_job_async):
    """Test that uncancelable job specified as async function can be
       canceled while it is not started yet."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job
        # Asserts that job is either pending or canceled.
        assert job.state in ['PENDING', 'CANCELED'], (
            'job that canceled immediately after submission has wrong '
            'state `%s`' % job.state
        )

    my_job_async.set_on_update(on_job_update)

    # Submit a job.
    new_job = await my_job_async.job(mustfail=False)

    # It must be canceled OK, because we are sure it has not started yet.
    my_job_async.job_manager_class.cancel(new_job.id)

    # Process ASGI messages but do not wait for jobs (no jobs started).
    await my_job_async.process_jobs()

    # Check a state of the job.
    assert job.state == 'CANCELED', ('Canceled job has wrong state '
                                     f'`{job.state}`!')


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_async_canceled(my_job_async):
    """Test that uncancelable job specified as async function cannot be
       canceled when it is working.

    Check that calling `cancel` when job is in the `WORKING`, `ERROR` or
    `DONE` state raises proper exception.
    """

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job

        if job.state in ['DONE', 'ERROR', 'WORKING']:
            canceled = my_job_async.job_manager_class.cancel(job.id)
            assert not canceled, (
                f'Uncancelable job is canceled in the `{job.state}` state!')

    my_job_async.set_on_update(on_job_update)

    # Submit a job that fails.
    await my_job_async.job(mustfail=True)

    # Process ASGI messages and wait for the job to finish.
    await my_job_async.process_jobs()

    # Check a state of the job.
    assert job.state == 'ERROR', f'Failed job has wrong state `{job.state}`!'

    # Submit a job that succeeds.
    await my_job_async.job(mustfail=False)

    # Process ASGI messages and wait for the job to finish.
    await my_job_async.process_jobs()

    # Check a state of the job.
    assert job.state == 'DONE', f'Finished job has wrong state `{job.state}`!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_async_gen_done(my_job_async_gen):
    """Test cancelable job specified as async generator.

    Features:
      - Make sure job state tells the job is cancelable when job is in
        `PENDING`, `WORKING`, and `CANCELING` state and is not
        cancelable in other states.
      - Check the number of times job update handler called.
      - Check job state before and after its execution.
    """

    # Set up callback to get notifications when job state changes.
    job = None
    job_update_counter = 0

    def on_job_update(_job):
        """The callback updates `job` and `job_update_counter`."""
        nonlocal job, job_update_counter
        job = _job
        job_update_counter += 1
        # Make sure job state tells the job is cancelable when job is in
        # `PENDING`, `WORKING`, and `CANCELING` state and is not
        # cancelable in other states.
        if job.state in ['PENDING', 'WORKING', 'CANCELING']:
            assert job.is_cancelable, ('Job is not cancelable when it '
                                       'must be cancelable!')
        else:
            assert not job.is_cancelable, ('Job is cancelable when it '
                                           'must not be cancelable!')

    my_job_async_gen.set_on_update(on_job_update)

    # Submit a job which must finish OK.
    new_job = await my_job_async_gen.job(yieldsteps=1, mustfail=False)

    # Check that job is cancelable.
    assert new_job.is_cancelable, ('Job instance states that cancelable job '
                                   'is not cancelable!')

    # Check job state right after job is submitted.
    assert job.state == 'PENDING', ('Submitted job has wrong state '
                                    f'{job.state}!')

    # Process ASGI messages and wait for the job to finish.
    await my_job_async_gen.process_jobs()

    # Check job state when job is done.
    assert job.state == 'DONE', f'Finished job has wrong state `{job.state}`!'

    # Check that job update callback has been called four times:
    #   1. job is submitted
    #   2. job switches to the working state
    #   3. job reports the progress
    #   4. job finishes
    assert job_update_counter == 4, 'Incorrect number of job updates detected!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_async_gen_failed(my_job_async_gen):
    """Test job specified as async generator which ends with error."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job

    my_job_async_gen.set_on_update(on_job_update)

    # Submit a job which must fail.
    await my_job_async_gen.job(yieldsteps=1, mustfail=True)

    # Process ASGI messages and wait for the job to finish.
    await my_job_async_gen.process_jobs()

    # Check job state when job is done.
    assert job.state == 'ERROR', f'Failed job has wrong state `{job.state}`!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_async_gen_discarded(my_job_async_gen):
    """Check that job specified as async generator function will not
       even start if canceled before it has started."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job
        # Asserts that job is either pending or canceled.
        assert job.state in ['PENDING', 'CANCELED'], (
            'Job that canceled immediately after submission has wrong '
            f'state `{job.state}`!')

    my_job_async_gen.set_on_update(on_job_update)

    # Submit a job which must fail immediately.
    new_job = await my_job_async_gen.job(yieldsteps=0, mustfail=True)

    # Cancel the job before it has chance to start.
    my_job_async_gen.job_manager_class.cancel(new_job.id)

    # Process ASGI messages but do not wait for jobs (no jobs started).
    await my_job_async_gen.process_jobs()

    # Check a state of the job.
    assert job.state == 'CANCELED', ('Canceled job has wrong state '
                                     f'`{job.state}`!')


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_job_async_gen_canceled(my_job_async_gen):
    """Check the job specified as async generator can be canceled in the
       middle of an execution.

    In this test we need to be sure that we cancel already running job
    (i.e. in the `WORKING` state). This is important because job which
    is not started simply changes its state from `PENDING` to `CANCELED`
    directly (and that case is checked in other tests). Here we test
    that job can (or cannot) be canceled in the middle of execution.
    """

    # Setup callback which is invoked when job state changes.
    job = None
    job_update_counter = 0

    def on_job_update(_job):
        """Here we cancel the job when it is started."""
        nonlocal job, job_update_counter

        # Cancel the job when it updates in the `WORKING` state for the
        # second time. We do it just to be sure it is somewhere in the
        # middle of execution.
        if (job is not None and
                _job.state == job.state == 'WORKING'):
            my_job_async_gen.job_manager_class.cancel(job.id)

        job = _job
        job_update_counter += 1

    my_job_async_gen.set_on_update(on_job_update)

    # Submit a job which must fail and tell it to do multiple iterations.
    await my_job_async_gen.job(yieldsteps=42, mustfail=True)

    # Process ASGI messages and wait for the job to finish.
    await my_job_async_gen.process_jobs()

    # Check a state of the job.
    assert job.state == 'CANCELED', ('Canceled job has wrong state '
                                     f'`{job.state}`!')

    # Check number of updates:
    # PENDING -> WORKING (x2) -> CANCELING (x2) -> CANCELED
    assert job_update_counter == 6, 'Incorrect number of job updates detected!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_jobfunc_signature_checked(my_job):
    """Test that function signature is checked when job is submitted."""

    # Submit a job with wrong signature and assure the exception raised.

    with pytest.raises(TypeError,
                       message='Job function signature is not checked!'):
        await my_job.job()  # `mustfail` is missing

    with pytest.raises(TypeError,
                       message='Job function signature is not checked!'):
        await my_job.job(extra_kwarg=42, mustfail=True)

    with pytest.raises(TypeError,
                       message='Job function signature is not checked!'):
        await my_job.job(42, mustfail=True)

    with pytest.raises(TypeError,
                       message='Job function signature is not checked!'):
        await my_job.job(42)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_misusage(my_job):
    """Test different corner cases caused by incorrect usage.

      - Check that it is impossible to make two job managers with the
        same name.
      - Check that it is impossible to register two job functions with
        the same name inside a single job manager, but it is possible in
        the different job managers.
      - Check that it is impossible to submit a job with an identifier
        which is already used by some other job.
    """

    # Disable some pylint checks, they are redundant here in the test.
    # pylint: disable=missing-docstring,unused-variable,function-redefined

    # Check that namesake job managers are not allowed.
    class MyNamesakeJobManager(jobmanager.JobManager):
        pass
    with pytest.raises(AssertionError,
                       message='Successfully made two job managers with the '
                               'same names!'):
        class MyNamesakeJobManager(jobmanager.JobManager):
            pass

    # Check that it is impossible to register two job functions with the
    # same name inside a single job manager, but it is possible in the
    # different job managers.
    class MyJobManager1(jobmanager.JobManager):
        pass

    class MyJobManager2(jobmanager.JobManager):
        pass

    @MyJobManager1.job()
    def my_namesake_job():
        pass

    @MyJobManager2.job()
    def my_namesake_job():
        pass

    with pytest.raises(AssertionError,
                       message='Successfully registered two job function with '
                               'the same name!'):
        @MyJobManager1.job()
        def my_namesake_job():
            pass

    # Check that it is impossible to submit a job with an identifier
    # which is already used by some other job.

    # Submit 2 jobs with the same identifier and check that second
    # submission raises an exception.
    job_id = uuid.uuid4()

    def on_job_submit(job):
        job.id = job_id
        return job
    my_job.set_on_submit(on_job_submit)

    # Submit a job #1.
    await my_job.job(mustfail=False)

    with pytest.raises(django.db.utils.IntegrityError,
                       message='Successfully submitted a job with the '
                               'identifier that has already been used for '
                               'another job!'):
        # Submit a job #2.
        await my_job.job(mustfail=False)

    # Process ASGI messages and wait for the job to finish.
    await my_job.process_jobs()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_progress_objects(my_job_manager):
    """Test different kinds of progress objects yielded from the job.

    Features:
      - Test partial progress events, if some value is None or absent
        then it does not change.
      - Test that job can yield `dict` and `None`.
      - Test that initial empty values.
    """

    # Test plan: list of pairs, where the first item in each pair stands
    # for the object yielded from a job function, second - for the
    # expected job progress information: message, payload, readiness.
    test_plan = [
        [{},
         {'message': '', 'payload': {}, 'readiness': None}],
        [{'message': 'msg1', 'payload': {'a': 'b'}, 'readiness': 0.1},
         {'message': 'msg1', 'payload': {'a': 'b'}, 'readiness': 0.1}],
        [{'readiness': 0.2},
         {'message': 'msg1', 'payload': {'a': 'b'}, 'readiness': 0.2}],
        [{'message': None, 'payload': {'a': 2}, 'readiness': 0.4},
         {'message': 'msg1', 'payload': {'a': 2}, 'readiness': 0.4}],
        [{},
         {'message': 'msg1', 'payload': {'a': 2}, 'readiness': 0.4}],
        [{'message': None, 'payload': None, 'readiness': None},
         {'message': 'msg1', 'payload': {'a': 2}, 'readiness': 0.4}],
        [{'readiness': 1.0},
         {'message': 'msg1', 'payload': {'a': 2}, 'readiness': 1.0}],
        [None,
         {'message': 'msg1', 'payload': {'a': 2}, 'readiness': 1.0}],
    ]
    # We use `list.pop` so reverse the plan to start from the end.
    test_plan.reverse()

    job = None

    def on_job_update(_job):  # pylint: disable=arguments-differ
        nonlocal job
        job = _job
        if test_plan and test_plan[-1][0] is None:
            # Extract next `test_plan` element and take its second
            # part which contains expected progress state.
            gold_progress = test_plan.pop()[1]
            assert job.message == gold_progress['message']
            assert job.payload == gold_progress['payload']
            assert job.readiness == gold_progress['readiness']

    my_job_manager.set_on_update(on_job_update)

    @my_job_manager.job_manager_class.job()
    async def my_job():
        """Test job that yields different types of progress objects."""

        while test_plan:
            to_yield = test_plan[-1][0]
            test_plan[-1][0] = None
            yield to_yield

    # Submit a job which must finish OK.
    await channels.db.database_sync_to_async(my_job)()

    # Process ASGI messages and wait for the job to finish.
    await my_job_manager.process_jobs()

    # It is important to check the result code, because asserts in the
    # `on_job_update` handler actually lead to the `ERROR` status of the
    # job.
    assert job.state == 'DONE', f'Job has failed: `{job.payload}`!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_default_channel_name():
    """Test that the default channel name is constructed correctly."""

    class JobManager(jobmanager.JobManager):
        """Job manager to test on."""
        pass

    assert (
        JobManager.job_channel_name
        == 'test_default_channel_name._locals_.jobmanager'
    ), 'Automatically generated channel name is wrong!'


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_concurrent_jobs(my_job):
    """Test that jobs are processed in the order they were submitted.

    Features:
      - Use different ways to set up identifiers, make sure on update
        and on submit handlers receives identifiers as `UUID` instances.
    """

    # The log of job status changes, grouped by job id cause jobs run
    # asynchronously.
    job_event_log = {}

    def on_job_update(job):
        """The callback to log events into `job_event_log`."""
        assert isinstance(job.id, uuid.UUID), 'Job id is not UUID!'
        job_log = job_event_log.setdefault(job.id, [])
        job_log.append(job.state)

    my_job.set_on_update(on_job_update)

    def on_job_submit(job):
        assert isinstance(job.id, uuid.UUID), 'Job id is not UUID!'
        job.id = job.kwds.pop('job_id')
        return job

    my_job.set_on_submit(on_job_submit)

    # Submit 3 jobs (with fancy identifiers) to check that they are processed
    # in the order of submission.
    # Submit a job #1.
    job1_id = '11111111-1111-1111-1111-111111111111'
    await my_job.job(mustfail=False, job_id=job1_id)

    # Submit a job #2.
    job2_id = '{22222222-2222-2222-2222-222222222222}'
    await my_job.job(mustfail=False, job_id=job2_id)

    # Submit a job #3.
    job3_id = uuid.UUID('33333333-3333-3333-3333-333333333333')
    await my_job.job(mustfail=False, job_id=job3_id)

    # Process ASGI messages and wait for the jobs to finish.
    await my_job.process_jobs(jobs=3)

    expected_log = {
        uuid.UUID('11111111-1111-1111-1111-111111111111'):
            ['PENDING', 'WORKING', 'DONE'],
        uuid.UUID('22222222-2222-2222-2222-222222222222'):
            ['PENDING', 'WORKING', 'DONE'],
        uuid.UUID('33333333-3333-3333-3333-333333333333'):
            ['PENDING', 'WORKING', 'DONE'],
    }

    assert job_event_log == expected_log, (
        'Log of concurrent jobs status changes are different from '
        'the expected one!')


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_cancel_alias(my_job):
    """Test that alias `job_func.cancel` works."""

    # Set up callback to get notifications when job state changes.
    job = None

    def on_job_update(_job):
        """The callback to update `job`."""
        nonlocal job
        job = _job
        # Asserts that job is either pending or canceled.
        assert job.state in ['PENDING', 'CANCELED'], (
            'Job that canceled immediately after submission has wrong '
            f'state `{job.state}`!')

    my_job.set_on_update(on_job_update)

    # Submit a job.
    new_job = await my_job.job(mustfail=False)

    # Test `cancel` alias on the job function.
    my_job.job_orig.cancel(new_job.id)

    # Process ASGI messages but do not wait for jobs (no jobs started).
    await my_job.process_jobs()

    # Check a state of the job.
    assert job.state == 'CANCELED', ('Canceled job has wrong state '
                                     f'`{job.state}`!')


# -------------------------------------------------------------------- FIXTURES


@pytest.fixture
async def my_job_manager():
    """Fixture provides `JobManager` subclass for tests.

    Yields:
        Object with the following attributes:
            job_manager_class: The`JobManager` subclass.
            set_on_update: Function which accepts callback invoked each
                time job state changes. The callbackmust accept a single
                argument - `Job` instance.
            process_jobs: Function which make the job manager to
                process a message.
    """

    class MyJobManager(jobmanager.JobManager):
        """Job manager to test on."""

        job_channel_name = uuid.uuid4().hex

        # Event handlers set from tests.
        _on_update = None
        _on_submit = None
        # Counter of finished jobs.
        finished_jobs = set()

        @classmethod
        def on_job_submit(cls, job):
            if cls._on_submit is not None:
                return cls._on_submit(job)
            return job

        @classmethod
        def on_job_update(cls, job):
            if job.state in ['DONE', 'ERROR', 'CANCELED']:
                cls.finished_jobs.add(job.id)
            if cls._on_update is not None:
                cls._on_update(job)

        @classmethod
        def set_on_submit(cls, on_submit_callback):
            """Set job submit handler callback."""
            cls._on_submit = on_submit_callback

        @classmethod
        def set_on_update(cls, on_update_callback):
            """Set job update handler callback."""
            cls._on_update = on_update_callback

    comm = ch_testing.ApplicationCommunicator(MyJobManager, scope={})

    async def process_jobs(*, jobs=1):
        """Let `MyJobManager` receive ASGI messages and wait for jobs.

        This function mimicates Channels layer and routes `jobs` number
        of messages from the channel to the consumer. Then it waits
        until all that jobs finish.

        Args:
            jobs: The number of jobs to process and wait.
        """
        # Here comes the magic! Extract a message from the channel
        # designated for `MyJobManager` and dispatch it to the
        # `MyJobManager` through the communicator. This hack allows to
        # test JobManager subclass without running the separate worker
        # process.
        for _ in range(jobs):
            message = await ch_layers.get_channel_layer().receive(
                MyJobManager.job_channel_name
            )
            await comm.send_input(message)
        # Wait until all job manager jobs finish.
        while len(comm.instance.finished_jobs) != jobs:
            await asyncio.sleep(0.01)
        # Cleanup finished jobs for the case when `process_jobs` is run
        # multiple times in a single test.
        comm.instance.finished_jobs.clear()

    yield types.SimpleNamespace(job_manager_class=MyJobManager,
                                set_on_update=MyJobManager.set_on_update,
                                set_on_submit=MyJobManager.set_on_submit,
                                process_jobs=process_jobs)

    # Properly shutdown the consumer (as ASGI application).
    comm.stop()
    await comm.wait()


@pytest.fixture
async def my_job(my_job_manager):
    """Fixture provides the job definition (regular function).

    Returns:
        The object yielded by the fixture `my_job_manager` with one
        extra attribute: `job` - job function decorated with `@job` and
        wrapped into `sync_to_async` for convenience (tests are async).
    """

    @my_job_manager.job_manager_class.job()
    def my_job_func(*, mustfail):
        """Simple uncancelable job function."""

        if mustfail:
            raise RuntimeError('Job failed, as requested!')

        return {
            'message': 'Job well done!',
            'payload': {'coolstuff': 'here'},
            'readiness': 1.0
        }

    my_job_manager.job_orig = my_job_func
    my_job_manager.job = channels.db.database_sync_to_async(my_job_func)
    return my_job_manager


@pytest.fixture
async def my_job_gen(my_job_manager):
    """Fixture provides the job definition (generator).

    Returns:
        The object yielded by the fixture `my_job_manager` with one
        extra attribute: `job` - job function decorated with `@job` and
        wrapped into `sync_to_async` for convenience (tests are async).
    """

    @my_job_manager.job_manager_class.job()
    def my_job_gen(yieldsteps, *, mustfail):
        """Job function which yields the progress."""

        for i in range(yieldsteps):
            progress = {
                'message': 'step %s or %s' % (i + 1, yieldsteps),
                'payload': dict({'step': i + 1, 'total': yieldsteps}),
                'readiness': (i + 1) / yieldsteps,
            }
            yield progress

        if mustfail:
            raise RuntimeError('Job failed, as requested!')

    my_job_manager.job_orig = my_job_gen
    my_job_manager.job = channels.db.database_sync_to_async(my_job_gen)
    return my_job_manager


@pytest.fixture
async def my_job_async(my_job_manager):
    """Fixture provides the job definition (async function).

    Returns:
        The object yielded by the fixture `my_job_manager` with one
        extra attribute: `job` - job function decorated with `@job` and
        wrapped into `sync_to_async` for convenience (tests are async).
    """

    @my_job_manager.job_manager_class.job()
    async def my_job_async(mustfail):
        """Async uncancelable job function."""
        if mustfail:
            raise RuntimeError('Job failed, as requested!')

        return {
            'message': 'job well done',
            'payload': {'coolstuff': 'here'},
            'readiness': 1.0
        }

    my_job_manager.job_orig = my_job_async
    my_job_manager.job = channels.db.database_sync_to_async(my_job_async)
    return my_job_manager


@pytest.fixture
async def my_job_async_gen(my_job_manager):
    """Fixture provides the job definition (async generator).

    Returns:
        The object yielded by the fixture `my_job_manager` with one
        extra attribute: `job` - job function decorated with `@job` and
        wrapped into `sync_to_async` for convenience (tests are async).
    """

    @my_job_manager.job_manager_class.job()
    async def my_job_async_gen(yieldsteps, *, mustfail):
        """Job function which yields the progress."""

        for i in range(yieldsteps):
            progress = {
                'message': 'step %s or %s' % (i + 1, yieldsteps),
                'payload': dict({'step': i + 1, 'total': yieldsteps}),
                'readiness': (i + 1) / yieldsteps,
            }
            yield progress

        if mustfail:
            raise RuntimeError('Job failed, as requested!')

    my_job_manager.job_orig = my_job_async_gen
    my_job_manager.job = channels.db.database_sync_to_async(my_job_async_gen)
    return my_job_manager
