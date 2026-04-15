"""Shared pytest fixtures for the unit test suite."""

from datetime import date

import pandas as pd
import pytest
from pytest_mock import MockerFixture


# ---------------------------------------------------------------------------
# Pre-built ANBIMA holiday data used to seed the in-process calendar cache.
# The DATE column keeps the raw timestamp-string format expected by
# DatesBRAnbima.transform_holidays so the fixture is transparent to callers.
# ---------------------------------------------------------------------------
_ANBIMA_HOLIDAYS_RAW = pd.DataFrame(
	{
		"DATE": [
			"2024-01-01 00:00:00",
			"2024-04-21 00:00:00",
			"2024-09-07 00:00:00",
			"2024-11-02 00:00:00",
			"2024-11-15 00:00:00",
			"2024-12-25 00:00:00",
		],
		"WEEKDAY": [
			"Segunda-feira",
			"Domingo",
			"Sabado",
			"Sabado",
			"Sexta-feira",
			"Quarta-feira",
		],
		"NAME": [
			"Confraternizacao Universal",
			"Tiradentes",
			"Independencia do Brasil",
			"Finados",
			"Proclamacao da Republica",
			"Natal",
		],
	}
)


@pytest.fixture(autouse=True)
def _ensure_anbima_cache(mocker: MockerFixture) -> None:
	"""Guarantee the ANBIMA calendar cache is always available.

	Some xdist workers start without a warm disk cache.  When those workers
	run a test whose ``mock_fast_operations`` autouse fixture has already
	patched ``requests.get``, the fallback HTTP fetch returns
	``b"test content"`` which pandas cannot parse as Excel, causing a
	``ValueError: Excel file format cannot be determined`` that contaminates
	unrelated B3 ingestion tests.

	This fixture intercepts ``CacheManager._load_cache`` and returns
	pre-built holiday data whenever:

	* the requested key is ``"br_anbima_holidays_raw"`` (the ANBIMA key), AND
	* ``bool_reuse_cache is True`` on that ``CacheManager`` instance.

	The condition on ``bool_reuse_cache`` preserves the original behaviour for
	calendar-specific tests (``test_calendar_br.py``) that create
	``DatesBRAnbima(bool_reuse_cache=False, bool_persist_cache=False)`` and
	mock ``requests.get`` themselves.

	Parameters
	----------
	mocker : MockerFixture
		pytest-mock fixture providing the mocker.patch interface.
	"""
	from stpstone.utils.cache.cache_manager import CacheManager

	_original_load = CacheManager._load_cache

	def _smart_load(self: CacheManager, key: str) -> pd.DataFrame | None:
		"""Return pre-built holidays when the ANBIMA cache key is requested.

		Parameters
		----------
		key : str
			Cache key being requested.

		Returns
		-------
		pd.DataFrame | None
			Pre-built holiday DataFrame for the ANBIMA key, or the result
			of the original ``_load_cache`` call for all other keys.
		"""
		if key == "br_anbima_holidays_raw" and self.bool_reuse_cache:
			return _ANBIMA_HOLIDAYS_RAW
		return _original_load(self, key)

	mocker.patch.object(CacheManager, "_load_cache", new=_smart_load)


@pytest.fixture(autouse=True)
def _fast_backoff(mocker: MockerFixture) -> None:
	"""Prevent real retry delays by making backoff give up immediately.

	backoff decorators are applied at class-definition time (module import),
	so patching ``backoff.on_exception`` inside individual fixtures has no
	effect on already-decorated methods.  Patching ``backoff._sync._next_wait``
	instead triggers backoff's own give-up branch on the very first retry:

	.. code-block:: python

	    try:
	        seconds = _next_wait(...)
	    except StopIteration:          # ← our mock raises this
	        _call_handlers(on_giveup, ...)
	        raise e                     # original exception propagates

	The patch is harmless for happy-path tests — ``_next_wait`` is only called
	when a decorated function raises an exception matching the backoff type.

	Parameters
	----------
	mocker : MockerFixture
		pytest-mock fixture providing the mocker.patch interface.
	"""
	mocker.patch("backoff._sync._next_wait", side_effect=StopIteration)
