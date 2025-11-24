"""Test routing logic and LLM domain classification."""

from unittest.mock import MagicMock, patch

import pytest

from app.algorithms.routing import choose_worker_group, get_domain_from_llm
from app.utils.schemas import PostEnvelope


class TestGetDomainFromLLM:
    """Test LLM domain classification."""

    @pytest.mark.asyncio
    async def test_llm_returns_valid_domain(self):
        """Test LLM returns a valid domain."""
        mock_content = MagicMock()
        mock_content.text = "health"
        mock_message = MagicMock()
        mock_message.content = [mock_content]

        mock_client = MagicMock()
        mock_client.messages.create = MagicMock(return_value=mock_message)

        with patch("app.algorithms.routing._get_groq_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            domain = await get_domain_from_llm("COVID-19 vaccine news")
            assert domain == "health"

    @pytest.mark.asyncio
    async def test_llm_returns_invalid_domain(self):
        """Test fallback when LLM returns invalid domain."""
        mock_content = MagicMock()
        mock_content.text = "invalid_domain"
        mock_message = MagicMock()
        mock_message.content = [mock_content]

        mock_client = MagicMock()
        mock_client.messages.create = MagicMock(return_value=mock_message)

        with patch("app.algorithms.routing._get_groq_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            domain = await get_domain_from_llm("Some random text")
            assert domain == "general"

    @pytest.mark.asyncio
    async def test_llm_error_fallback(self):
        """Test fallback when LLM call fails."""
        mock_client = MagicMock()
        mock_client.messages.create = MagicMock(side_effect=Exception("API Error"))

        with patch("app.algorithms.routing._get_groq_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            domain = await get_domain_from_llm("Test text")
            assert domain == "general"


class TestChooseWorkerGroup:
    """Test worker group selection."""

    @pytest.mark.asyncio
    async def test_explicit_domain_in_metadata(self):
        """Test explicit domain from metadata is used."""
        post = PostEnvelope(
            post_id="test-1", text="Random text", meta={"domain": "finance"}
        )

        worker_group = await choose_worker_group(post)
        assert worker_group == "finance-workers"

    @pytest.mark.asyncio
    async def test_llm_routing_for_health(self):
        """Test LLM routing for health content."""
        mock_content = MagicMock()
        mock_content.text = "health"
        mock_message = MagicMock()
        mock_message.content = [mock_content]

        mock_client = MagicMock()
        mock_client.messages.create = MagicMock(return_value=mock_message)

        with patch("app.algorithms.routing._get_groq_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            post = PostEnvelope(
                post_id="test-3", text="New vaccine shows promising results", meta={}
            )

            worker_group = await choose_worker_group(post)
            assert worker_group == "health-workers"
