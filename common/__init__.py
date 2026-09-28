"""Shared utilities: configuration, structured logging, and the simulated clock.

Imported by sources/, processing/, serving/ and orchestration/. Pure Python --
nothing here depends on Spark, so it loads anywhere.

Import from the submodules directly, e.g.:

    from common.config import load_config
    from common.sim_clock import SimClock
"""
