from __future__ import annotations
import asyncio
import pytest
from peercolab_engine import InMemoryContextCache, CallInformation


class FailingContextCache:
    async def put(self, transaction_id, ctx):
        return False

    async def get(self, transaction_id):
        return None


class ThrowingContextCache:
    async def put(self, transaction_id, ctx):
        raise RuntimeError("Cache connection failed")

    async def get(self, transaction_id):
        raise RuntimeError("Cache connection failed")


class TestInMemoryContextCache:
    @pytest.mark.asyncio
    async def test_put_and_get_basic_flow(self):
        cache = InMemoryContextCache()
        info = CallInformation.new("en-US", "tenant", "tx-1")
        ok = await cache.put("tx-1", info)
        assert ok is True
        retrieved = await cache.get("tx-1")
        assert retrieved is not None
        assert retrieved.locale == "en-US"
        assert retrieved.data_tenant == "tenant"

    @pytest.mark.asyncio
    async def test_returns_none_for_missing_key(self):
        cache = InMemoryContextCache()
        result = await cache.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_put_overwrites_existing_entry(self):
        cache = InMemoryContextCache()
        info1 = CallInformation.new("en-US", "tenant1", "tx-1")
        info2 = CallInformation.new("nb-NO", "tenant2", "tx-1")
        await cache.put("tx-1", info1)
        await cache.put("tx-1", info2)
        retrieved = await cache.get("tx-1")
        assert retrieved.locale == "nb-NO"
        assert retrieved.data_tenant == "tenant2"

    @pytest.mark.asyncio
    async def test_stores_multiple_independent_entries(self):
        cache = InMemoryContextCache()
        info1 = CallInformation.new("en-US", "tenant1", "tx-1")
        info2 = CallInformation.new("nb-NO", "tenant2", "tx-2")
        await cache.put("tx-1", info1)
        await cache.put("tx-2", info2)
        r1 = await cache.get("tx-1")
        r2 = await cache.get("tx-2")
        assert r1.locale == "en-US"
        assert r2.locale == "nb-NO"

    @pytest.mark.asyncio
    async def test_expires_entry_after_max_lifetime_ms(self):
        cache = InMemoryContextCache(50)
        info = CallInformation.new("en-US")
        await cache.put("tx-expire", info)
        assert await cache.get("tx-expire") is not None
        await asyncio.sleep(0.07)
        assert await cache.get("tx-expire") is None


class TestFailingContextCache:
    @pytest.mark.asyncio
    async def test_put_returns_false(self):
        cache = FailingContextCache()
        result = await cache.put("tx-1", CallInformation.new("en-GB"))
        assert result is False

    @pytest.mark.asyncio
    async def test_get_returns_none(self):
        cache = FailingContextCache()
        result = await cache.get("tx-1")
        assert result is None


class TestThrowingContextCache:
    @pytest.mark.asyncio
    async def test_put_throws(self):
        cache = ThrowingContextCache()
        with pytest.raises(RuntimeError, match="Cache connection failed"):
            await cache.put("tx-1", CallInformation.new("en-GB"))

    @pytest.mark.asyncio
    async def test_get_throws(self):
        cache = ThrowingContextCache()
        with pytest.raises(RuntimeError, match="Cache connection failed"):
            await cache.get("tx-1")
