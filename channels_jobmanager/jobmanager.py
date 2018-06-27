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

"""Job manager for Django implemented on Channels."""

import asyncio
import collections
import functools
import inspect
import logging
import re
import traceback
import uuid

import asgiref.sync
import channels.consumer
import channels.db
import channels.layers
import django.utils.timezone
import namedlist


# Module level logger.
log = logging.getLogger(__name__)


class JobManager(channels.consumer.AsyncConsumer):
    """Subclass this class to create cancelable long-running jobs.

    Basic usage:
    ```
        # Create `JobManager` subclass:
        class MyJobs(channels_jobmanager.JobManager):
            pass

        # Setup Channels routing:
        application = channels.routing.ProtocolTypeRouter({
            'channel': channels.routing.ChannelNameRouter({
                MyJobs.job_channel_name: MyJobs
            })
        })

        # Declare some job function:
        @MyJobs.job()
        def my_job(count):
            for i in range(count):
                # Do something here and
                yield  # Raises `GeneratorExit` if canceled.
    ```
    For slightly more complex example see `example/example.py`.

    The workflow is the following. When decorated job function is run,
    its arguments serialized to the database (arguments must be
    JSON-serializable) and channel message is sent to the channel
    corresponding to the `JobManager` subclass. Decorated job function
    returns an instance of job model which holds all the information
    about new submitted job, for example the job `id`. Meanwhile some
    worker process receives the message (though the ASGI layer, i.e.
    Redis), takes the oldest pending job from the database and executes
    it.

    Job functions can be synchronous or asynchronous and also they can
    be interruptible or not interruptible.

    Synchronous functions are run in the separate threads inside the
    worker process. Asynchronous ones run in the main even loop and
    shall never block it (otherwise worker will not receive new Channel
    messages). NOTE: If you need to get access to the database or some
    other potentially long operation you have to offload this code to
    some working thread. Channels help us with this by implementing
    `channels.db.database_sync_to_async`. To simplify jobmanager usage
    each jobmanager class contains an alias of this function named
    `sync_to_async`.

    To make job interruptible it shall `yield` from time to time. At
    this point `JobManager` checks if job is canceled and stop the job
    if so. In such cases `yield` raises `GeneratorExit` exception inside
    the job function.

    Job functions may report the progress. For that purpose they yield
    the object containing the following fields:
      - message: User message describing current job status.
      - payload: Dict containing details about current job status. The
            information stored here is job-specific and does not
            processed by the job execution routines. This field is used
            for both: intermediate progress reports and final job
            execution result.
      - readiness: A readiness of the job: floating point number from
            0.0 to 1.0 Can have value `NaN` which indicates that it is
            impossible to calculate readiness.

    Yielded object must be a Python dict with fields `message`,
    `payload`, and `readiness` (any of these can be omitted) or `None`.
    Uninterruptible jobs may return such progress object to report an
    execution result.

    To cancel the job call either `MyJobs.cancel(job_id=<job id>)`
    or `my_job.cancel(job_id=<job_id>)`.

    For convenience it is recommended to name subclasses with `Jobs`
    suffix. For example: `ProtocolJobs`, `ImageProcessingJobs`.

    There are several static fields which can be set in `JobManager`
    subclass for fine tunning:
      - job_channel_name: Name of the Channels channel used to notify
            the workers. A channel name may contain only alphanumerics,
            dashes, and periods. If `None` (which is by default) then
            name is generated automatically from the `JobManager`
            subclass `qualname`, by lowercasing it and replacing all
            wrong symbols with an underscore `_`.
            NOTE: This field can be read to setup routing, even if it
            is not specified explicitly in the subclass.
      - job_model_class: Specify job model class used to serialize jobs.
            Value can be class instance or callable returning class
            instance. Job model class must be inherited from
            `models.JobBase`. Default is the `models.Job`.
      - jobs_per_worker: The limit of jobs running in a single worker.
            Default is `None` which means there is not limit.
      - on_job_submit: Callable which receives a job model instance when
            job is submitted (i.e. decorated job function called). Here
            you can initialize/modify model fields. For example it is a
            proper place to set user, to modify values of job function
            arguments or to set additional fields to the job model (when
            custom model is set by `job_model_class`). Function must
            return job model instance.
      - on_job_update: Callable which receives a job model instance each
            time some job state changes. This is the place to watch over
            a job progress.
    """

    # --------------------------------------- SETTINGS TO SPECIFY IN SUBCLASSES

    # Job manager uses Django Channels to wake up jobbers when there is
    # a work to do. Each particular job manager (subclass of this class)
    # shall use different channel name. If the `job_channel_name` is
    # `None` the class name is used to generate it. A channel name may
    # contain only alphanumerics, dashes, and periods.
    job_channel_name = None

    # Job model class. Set this to another `JobBase` subclass to add new
    # fields to the job. If `None` then `models.Job` is used.
    job_model_class = None

    # The maximum number of jobs a single worker runs at the same time.
    jobs_per_worker = None

    # -------------------------------- EVENT HANDLERS TO OVERRIDE IN SUBCLASSES

    @classmethod
    def on_job_submit(cls, job):
        """Overwrite this to customize job model."""
        return job

    @classmethod
    def on_job_update(cls, job):
        """Overwrite this to be notified when job updates."""
        pass

    # ----------------------------- ALIAS TO RUN SYNC FUNCTION IN ASYNC CONTEXT
    sync_to_async = channels.db.database_sync_to_async

    # -------------------------------------------------- JOB FUNCTION DECORATOR

    @classmethod
    def job(cls, on_job_submit=None):
        """Decorator to mark job functions.

        This decorator is intended to be used to mark function as a job
        function. Job functions will not be run immediately when called.
        Instead argument of the function will be stored in the database
        and jobber (worker) will be notified (through the channel) that
        there is a job to do. So eventually, the job function is called
        in a designated Django Channels worker.

        Args:
            on_job_submit: Callable which is invoked each time new job
                is submitted. This callable receives a job mode instance
                and may modify its fields. The callable must return job
                model instance. The callable is invoked after the
                `JobManager.on_job_submit`.
        """

        def decorator(job_func):
            """Decorator - the result of `@job(...)` evaluation."""

            # Register job function in the registry. This happens for
            # each job function typically during module import. The most
            # important that this happens in each Channels worker, so if
            # such worker receives the message to start the job it will
            # find it in the registry.
            job_func_name = job_func.__name__
            assert cls.job_channel_name in cls._registry, (
                f'Job manager `{cls.job_channel_name}` is not registered '
                '`JobManager` subclass!'
            )
            jm_info = cls._registry[cls.job_channel_name]
            assert (job_func_name not in jm_info.job_funcs), (
                f'Job function with the name `{job_func_name}` is already '
                f'registered within job manager `{cls.job_channel_name}`!'
            )
            jm_info.job_funcs[job_func_name] = job_func
            assert inspect.isfunction(job_func), ('Job function is not a '
                                                  'function!')

            @functools.wraps(job_func)
            def wrapper(*args, **kwds):
                """Submit the job instead of running it."""

                job = cls.job_model_class()
                # Any `PENDING` job can be canceled.
                job.is_cancelable = True
                job.channel_name = cls.job_channel_name
                job.function_name = job_func_name
                job.args = list(args)
                job.kwds = dict(kwds)

                # Call on submit handlers. Each time assure the type of
                # the `id` is `UUID` in case some handler has changed
                # its value.
                job = cls.on_job_submit(job)
                job.id = uuid.UUID(str(job.id))
                if on_job_submit is not None:
                    job = on_job_submit(job)
                    job.id = uuid.UUID(str(job.id))

                # Check that given arguments match the job function
                # signature. It is important to do this check after job
                # submit handlers are invoked. They can modify job
                # function arguments.
                inspect.signature(job_func).bind(*job.args, **job.kwds)

                # Save with `force_insert` to ensure there is no job
                # with the same `id` in the database.
                job.save(force_insert=True)

                # Wake a jobber up by pinging the channel jobbers are
                # listening to. The jobber will wake up and take the
                # first pending job from the list.
                asgiref.sync.async_to_sync(cls._ping_jobber)()

                log.debug('New job `%s` submitted: %s', job.function_name, job)

                return job

            # Add an alias `cancel` so it is possible to call
            # `job_func.cancel()` instead of using JobManager class or
            # its subclasses.
            wrapper.cancel = cls.cancel

            return wrapper

        return decorator

    # ---------------------------------------------------------- CANCEL THE JOB

    @classmethod
    def cancel(cls, job_id):
        """Call this to cancel the job.

        Invocation of this function, marks job record in the database as
        `CANCELING` so it stops the next time job checks the state (or
        even did not start at all if state was 'PENDING'). If jobs state
        is not `WORKING` or `PENDING` then the function simply does
        nothing.

        NOTE: We intentionally do not raise any errors if job is not
        cancelable. Since job performs asynchronously it is simply
        impossible for the client to know if it is still cancelable or
        not.

        Args:
            job_id: Identifier of job which must be canceled.
        Returns:
            `True` if job has been successfully marked as canceling.
            `False` if job cannot be canceled by some reason, e.g. it
            has already stopped or the implementation does not allow
            cancelation.
        Raises:
            NoSuchJobError: Job with given id does not exist.
        """

        assert cls.job_channel_name is not None and cls is not JobManager, (
            'Method `cancel` is called on the `JobManager` class, it '
            'must be only called on the `JobManager` subclass!'
        )

        try:
            job = cls.job_model_class.objects.get(id=job_id)
        except cls.job_model_class.DoesNotExist:
            raise NoSuchJobError('Job `{job_id}` does not exist!')

        jobs_updated = 0

        if job.state == 'PENDING':
            jobs_updated = (cls.job_model_class.objects
                            .filter(id=job_id)
                            .filter(state='PENDING')
                            .filter(is_cancelable=True)
                            .update(state='CANCELED'))

        if job.state == 'WORKING':
            jobs_updated = (cls.job_model_class.objects
                            .filter(id=job_id)
                            .filter(state='WORKING')
                            .filter(is_cancelable=True)
                            .update(state='CANCELING'))

        assert jobs_updated <= 1, 'To many jobs updated!'

        # Manually trigger update notification cause `update` above does
        # not invoke Django `post_save` handlers automatically.
        if jobs_updated != 0:
            job.refresh_from_db()
            cls._send_job_update_notification(job)
        return jobs_updated != 0

    # ---------------------------------------------------------- IMPLEMENTATION

    def __init__(self, *args, **kwds):
        """Constructor."""

        super().__init__(*args, **kwds)

        # The list of asyncio tasks of currently running jobs.
        self._jobs = set()

    _JobManagerInfo = namedlist.namedlist(
        '_JobManagerInfo',
        [
            ('subclass', None),
            ('job_funcs', namedlist.FACTORY(dict)),
        ]
    )

    # Registry which holds all the `JobManager` subclasses and the job
    # functions which were registered with their help (by using `job`
    # decorator).
    _registry = collections.defaultdict(_JobManagerInfo)

    # Identifier of the particular `JobManager` subclass.
    _id = None

    # Pattern matches non non alphanumeric, not hyphen and not period.
    # Used to replace prohibited symbols while building a channel name.
    _JOB_CHANNEL_NAME_PATTERN = re.compile(r'[^a-zA-Z0-9_\-.]')

    @classmethod
    def __init_subclass__(cls, **kwds):
        """Register all subclasses in the registry.

        This classmethod is invoked each time Python parses `JobManager`
        subclass. Each such subclass is registered in the registry with
        channel name a an identifier.
        """

        # Import models inside function, because Django requires that
        # models are not imported with application (from application's
        # `__init__.py` directly on indirectly). But the fact is this
        # class is a Channels message consumer means that it is imported
        # in routing during application configuration time. So the only
        # solution is to import the models module here inside a
        # function.
        from . import models

        super().__init_subclass__(**kwds)

        # Make `job_channel_name` from the subclass class name if it is
        # not specified explicitly in the subclass.
        if cls.job_channel_name is None:
            # Replace wrong chars with underscores (e.g. in `<locals>`).
            cls.job_channel_name = (
                cls._JOB_CHANNEL_NAME_PATTERN.sub('_',
                                                  cls.__qualname__.lower())
            )

        # Register the subclass with `job_channel_name` as a key.
        assert cls.job_channel_name not in cls._registry, (
            f'JobManager subclass `{cls.job_channel_name}` is already '
            'registered!'
        )
        cls._registry[cls.job_channel_name].subclass = cls

        # Initialize class used to save jobs into the database.
        if cls.job_model_class is None:
            # Initialize `job_model_class` with default.
            cls.job_model_class = models.Job
        elif (callable(cls.job_model_class)
              and not isinstance(cls.job_model_class, type)):
            # It is callable, but not a class instance.
            cls.job_model_class = cls.job_model_class()
        # Check the job model is a `JobBase` subclass.
        assert issubclass(cls.job_model_class, models.JobBase), (
            f'Job model class `{cls.job_model_class.__qualname__}` is '
            f'not subclass of `{models.JobBase.__qualname__}`!'
        )

    def _pick_pending_job(self):
        """Pick a pending job and atomically mark it as `WORKING`.

        Method atomically finds an oldest `PENDING` job and marks it as
        `WORKING`. It also properly updates the fields `is_cancelable`
        and `started`.

        Sometimes method is called when there is nothing to do. It is
        completely normal, because we always wake up a jobber when a
        worker leaves a blocking state. This is needed to avoid the case
        when job took too long and Channels has thrown away the message
        indicating that there is a job to work on.

        Returns:
            An instance of job model or `None` if there are no `PENDING`
            jobs in the database.
        """

        # Pick the oldest pending job belonging to this job manager
        # class and mark it as `WORKING` so no other jobbers will take
        # it. If no such job exists - silently exit.
        job = None
        while job is None:
            try:
                # Find a candidate to work on.
                job_id = (self.job_model_class.objects
                          .filter(channel_name=self.job_channel_name)
                          .filter(state='PENDING')
                          .earliest('submitted')).id
            except self.job_model_class.DoesNotExist:
                # There is nothing to do - silently exit.
                log.debug('All jobs are processed - there are no '
                          '`PENDING` jobs left in the database.')
                return None

            # Try to mark a job as `WORKING`.
            jobs_updated = (self.job_model_class.objects
                            .filter(id=job_id)
                            .filter(channel_name=self.job_channel_name)
                            .filter(state='PENDING')
                            .update(state='WORKING'))
            assert jobs_updated <= 1, 'To many jobs updated!'
            # Job successfully marked as `WORKING` - we have work to do.
            if jobs_updated == 1:
                # The `on_job_update` notification will be sent below
                # when `is_cancelable` is updated.
                job = self.job_model_class.objects.get(id=job_id)

        # Get a job function from the registry.
        job_manager_funcs = self._registry[self.job_channel_name].job_funcs
        job_func = job_manager_funcs[job.function_name]

        # Determine whether the job function can be canceled.
        job.is_cancelable = (inspect.isgeneratorfunction(job_func) or
                             inspect.isasyncgenfunction(job_func))
        job.started = django.utils.timezone.now()

        # NOTE: Update only modified fields. We do not want to rewrite
        # `state` which can concurrently change (i.e. to `CANCELING`).
        job.save(update_fields=['started', 'is_cancelable'])

        return job

    def _jobber(self, job):
        """Jobber - worker function which does the job.

        Args:
            job: The job instance to process.
        """

        # Get a job function from the registry.
        job_manager_funcs = self._registry[self.job_channel_name].job_funcs
        job_func = job_manager_funcs[job.function_name]

        job_args = list(job.args)
        job_kwds = dict(job.kwds)

        # Execute job function and handle errors if any.
        try:
            # Job function specified as async generator function which
            # yields the progress and therefore can be canceled. Perform
            # async iterations as it is described in PEP 525:
            # https://www.python.org/dev/peps/pep-0525/#support-for-asynchronous-iteration-protocol
            if inspect.isasyncgenfunction(job_func):
                # Alias to shorten async-to-sync transformation.
                async_to_sync = asgiref.sync.async_to_sync

                agen_obj = job_func(*job_args, **job_kwds)

                # Iterate through async iterator `StopAsyncIteration`
                # indicates end of the iteration process.
                while True:
                    try:
                        progress = async_to_sync(agen_obj.asend)(None)
                    except StopAsyncIteration:
                        # Generator has finished by itself, so job is
                        # done. We do not care if state has changed
                        # (cancel request received), what done is done.
                        job.is_cancelable = False
                        job.state = 'DONE'
                        job.save(update_fields=['is_cancelable', 'state'])
                        break

                    # Refresh job instance from the database and check
                    # if cancel request is received.
                    job.refresh_from_db()
                    assert job.state in ['CANCELING', 'WORKING'], (
                        'Job which is processed by a jobber suddenly '
                        f'appears with a wrong state `{job.state}`!'
                    )
                    # Update job fields from the progress object yielded
                    # from the job function.
                    job = self._update_job_with_progress(job, progress)
                    # NOTE: Only selected fields are updated. If we just
                    # call `job.save()` we can overwrite concurrently
                    # changed `state`!
                    job.save(update_fields=['message', 'payload', 'readiness'])

                    if job.state == 'CANCELING':
                        # It was cancelable but it is not anymore.
                        job.is_cancelable = False
                        job.state = 'CANCELED'
                        job.save(update_fields=['is_cancelable', 'state'])
                        # Stop iterating over generator.
                        async_to_sync(agen_obj.aclose)()
                        break

            # Job function specified as coroutine function. This type of
            # job functions do not produce progress messages and cannot
            # be canceled. They also require eventloop to run the
            # function.
            elif inspect.iscoroutinefunction(job_func):
                job_func_sync = asgiref.sync.async_to_sync(job_func)
                result = job_func_sync(*job_args, **job_kwds)

                # Update job with info returned from the job function.
                job = self._update_job_with_progress(job, result)

                # Set job readiness to 100% in case job did not do it in
                # the returned progress object.
                job.readiness = 1.0

                # Job is done.
                job.state = 'DONE'
                job.save(force_update=True)

            # Job function specified as generator function. This
            # generator yields progress objects, and can be canceled.
            elif inspect.isgeneratorfunction(job_func):
                gen_obj = job_func(*job_args, **job_kwds)

                # Iterate through the generator object `StopIteration`
                # indicates end of the iteration process.
                while True:
                    try:
                        progress = gen_obj.send(None)
                    except StopIteration:
                        # Generator has finished by itself, so job is
                        # done. We do not care if state has changed
                        # (cancel request received), what done is done.
                        job.is_cancelable = False
                        job.state = 'DONE'
                        job.save(update_fields=['is_cancelable', 'state'])
                        # Stop iterating over generator.
                        break

                    # Refresh job instance from the database and check
                    # if cancel request is received.
                    job.refresh_from_db()
                    assert job.state in ['CANCELING', 'WORKING'], (
                        'Job which is processed by a jobber suddenly '
                        f'appears with a wrong state `{job.state}`!'
                    )
                    # Update job fields from the progress object yielded
                    # from the job function.
                    job = self._update_job_with_progress(job, progress)
                    # NOTE: Only selected fields are updated. If we just
                    # call `job.save()` we can overwrite concurrently
                    # changed `state`!
                    job.save(update_fields=['message', 'payload', 'readiness'])

                    if job.state == 'CANCELING':
                        # It was cancelable but it is not anymore.
                        job.is_cancelable = False
                        job.state = 'CANCELED'
                        job.save(update_fields=['is_cancelable', 'state'])
                        # Stop iterating over generator.
                        gen_obj.close()
                        break

            # Simple job function specified as regular Python function.
            # This type of job functions does not produce progress
            # messages and cannot be canceled. Just run it and wait for
            # the result.
            else:
                result = job_func(*job_args, **job_kwds)

                # Update job with info returned from the job function.
                job = self._update_job_with_progress(job, result)

                # Set job readiness to 100% in case job did not do it in
                # the returned progress object.
                job.readiness = 1.0

                # Job is done.
                job.state = 'DONE'
                job.save(force_update=True)

        except Exception as e:  # pylint: disable=broad-except
            # Extract info from the exception happened.
            error_message = ''.join(
                traceback.format_exception_only(type(e), e)
            )
            error_details = traceback.format_exc()
            log.info('Job `%s` finished with error: %s',
                     job.function_name, error_message)
            log.debug('Job `%s` finished with error: %s',
                      job.function_name, error_details)
            # Error happened: keep readiness as is, update state and set
            # payload with information about the error happened.
            job.state = 'ERROR'
            # Even if it was cancelable, it is not anymore.
            job.is_cancelable = False
            job.payload = {
                'type': type(e).__name__,
                'message': error_message,
                'details': error_details,
            }

            job.message = error_message
            job.save(force_update=True)

    @classmethod
    def _update_job_with_progress(cls, job, progress):
        """Update a job with info from a given progress instance.

        Args:
            job: Instance of the job model.
            progress: Progress value - dict with keys (any subset of)
                `message`, `payload`, `readiness`, or simply `None`.
        Returns:
            The same `job` instance with updated fields `message`,
            `payload`, and `readiness`.
        """
        default = {'message': None, 'payload': None, 'readiness': None}
        if progress is not None:
            default.update(progress)
        progress = default
        assert len(progress) == 3, ('Extra fields detected in the progress '
                                    f'dictionary `{progress}`!')
        if progress['message'] is not None:
            job.message = progress['message']
        if progress['payload'] is not None:
            job.payload = dict(progress['payload'])
        if progress['readiness'] is not None:
            job.readiness = progress['readiness']
        return job

    @classmethod
    async def _ping_jobber(cls):
        """Wake up some jobber by sending a message to the channel."""
        await channels.layers.get_channel_layer().send(
            channel=cls.job_channel_name,
            message={'type': 'job.wakeup'}
        )

    @classmethod
    def _send_job_update_notification(cls, job):
        """Send notification to appropriate job manager.

        This method finds proper `JobManager` subclass in the registry
        and notify it about update of the given job.
        """
        assert job.channel_name in cls._registry, (
            'Cannot find `JobManager` subclass corresponsing to the '
            f'given job instance name: `{repr(job)}`!'
        )
        subclass = cls._registry[job.channel_name].subclass
        subclass.on_job_update(job)

    # ----------------------------------------------- CHANNELS MESSAGE HANDLERS

    async def job_wakeup(self, message):
        """Message handler for: `job.wakeup`.

        Receiving `job.wakeup` means that there is probably a work to
        do, so we wake the jobber up to make him check the job queue.
        (Method is called by the Channels in the worker process.)
        """
        assert message == {'type': 'job.wakeup'}

        # Remove finished jobs before submitting a new one.
        self._jobs = {j for j in self._jobs if not j.done()}

        job = None

        # There is either no job per process limit or the limit is not
        # reached yet - submit a new job.
        if (self.jobs_per_worker is None
                or len(self._jobs) <= self.jobs_per_worker):

            # Get a job to work on.
            job = await self.sync_to_async(self._pick_pending_job)()
            # If there is a work to do - run jobber in background so we
            # will continue receiving `job.wakeup` messages.
            if job is not None:
                self._jobs.add(
                    asyncio.ensure_future(
                        self.sync_to_async(self._jobber)(job)
                    )
                )

        # If we have started some new job and jobs per worker limit is
        # now exceeded - stop accepting `job.wakeup` messages by waiting
        # for at least one job to finish.
        if (job is not None
                and self.jobs_per_worker is not None
                and len(self._jobs) >= self.jobs_per_worker):
            assert len(self._jobs) == self.jobs_per_worker
            jobs_finished, jobs_pending = await asyncio.wait(
                self._jobs,
                return_when=asyncio.FIRST_COMPLETED
            )
            self._jobs = set(jobs_pending)

            # We have been blocked by waiting some job to finish for
            # unknown amount of time, so probably some ping messages
            # were expired and lost. We know the number of "slots" got
            # free, and send the same number of ping messages. So in the
            # worst case (this worker is the only one with free "slots")
            # we will receive all that pings and continue running jobs.
            pings = [self._ping_jobber() for _ in range(len(jobs_finished))]
            await asyncio.wait(pings)

# ------------------------------------------------------------------ EXCEPTIONS


class JobManagerError(Exception):
    """Base class for all specific exceptions raised from JobManager."""
    pass


class NoSuchJobError(JobManagerError):
    """Error indicating that job does not exist."""
    pass


class WrongUsageError(JobManagerError):
    """Error indicating that `JobManager` has been used incorrectly."""
    pass


class PermissionDeniedError(JobManagerError):
    """Error indication that permission is denied."""
    pass
