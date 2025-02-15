import asyncio

import pytest

from dispatcher.control import Control


@pytest.mark.asyncio
@pytest.mark.benchmark(group="control")
def test_alive_benchmark(benchmark, with_full_server, conn_config):
    control = Control('test_channel', config=conn_config)

    def alive_check():
        r = control.control_with_reply('alive')
        assert r == [None]

    with with_full_server(4):
        benchmark(alive_check)
