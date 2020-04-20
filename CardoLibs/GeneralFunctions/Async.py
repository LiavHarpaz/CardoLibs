import asyncio
import functools
from typing import Any, Callable, Generator, List


def async_retry_function(retries: int, sleep_for: int) -> Callable:
	def inner(func: Callable):
		async def f_retry(*args, **kwargs):
			mtries = retries
			while mtries > 0:
				try:
					return await func(*args, **kwargs)
				except Exception as e:
					await asyncio.sleep(sleep_for)
					print("RETRY! exception: ", e, *args, kwargs)
					mtries -= 1
			raise Exception("exception in `async_retry_function`, exceeded maximum allowed failures")

		return f_retry

	return inner


def async_wrapper(func: Callable) -> Callable:
	@functools.wraps(func)
	async def inner(self, *args, **kwargs):
		return await asyncio.get_event_loop().run_in_executor(None, functools.partial(func, **kwargs), self, *args)

	return inner


def chunker(values: List, chunksize: int) -> Generator[List, Any, None]:
	for i in range(0, len(values), chunksize):
		yield values[i:i + chunksize]
