# Job manager for Django Channels

## Features

- Offload long-running functions (jobs) to Channels workers.
- Job may report the progress.
- Job can be canceled.
- Job can be implemented as sync or async function.

## Quick start

1. Add `channels_jobmanager` to your INSTALLED_APPS setting:

```python
INSTALLED_APPS = [
    ...
    'channels_jobmanager',
]
```

2. Subclass `JobManager`:

```python
class MyJobs(channels_jobmanager.JobManager):
    """Example job manager class."""
    # Allow anonymous user to submit jobs.
    allow_anonymous_user = True
    # Watch over job state changes.
    @classmethod
    def on_job_update(cls, job_info):
        """Here we receive job progress messages."""
        print('JOB STATE CHANGES:', job_info)
```

3. Create simple job function and decorate it with `job()` :

```python
@MyJobs.job()
async def my_job(count):
    """Simple counter job."""
    for i in range(count):
        # Simulate doing the job with sleep.
        await asyncio.sleep(delay=1)
        # Report progress.
        yield {
            'message': 'Making a good progress...',
            'payload': {'step index': i},
            'readiness': (i + 1) / count
        }
    # Report final result.
    yield {
        'message': 'Job well done!',
        'payload': {'steps made': count},
        'readiness': 1.0
    }
```

4. Submit the job by invoking the decorated job function:

```python
job_info = my_job(message='Hi!')
```

5. Cancel the job:

```python
my_job.cancel(job_id=job_info.job_id)
```

6. Create database models, run Django and Channels worker.

```bash
python manage.py migrate
python manage.py runserver
python manage.py runworker myjobs
```

## Contributing

This project is developed and maintained by DATADVANCE LLC. Please
submit an issue if you have any questions or want to suggest an
improvement.

## Acknowledgements

This work is supported by the Russian Foundation for Basic Research
(project No. 15-29-07043).
