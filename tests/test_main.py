"""Unit tests for main.py (handle_command, SUPPORTED_COMMANDS, etc.)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as main_module


class TestSupportedCommands:
    """Tests for SUPPORTED_COMMANDS constant."""

    def test_supported_commands_contains_expected(self):
        """SUPPORTED_COMMANDS includes all script commands and stop."""
        expected = {
            "remoteoperate",
            "teleoperate",
            "calibrate",
            "find_port",
            "read_device",
            "setup",
            "write_position",
            "stop",
        }
        assert main_module.SUPPORTED_COMMANDS == expected

    def test_supported_commands_is_frozenset(self):
        """SUPPORTED_COMMANDS is immutable."""
        assert isinstance(main_module.SUPPORTED_COMMANDS, frozenset)


class TestHandleCommand:
    """Tests for handle_command dispatch logic."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Cyberwave client."""
        client = MagicMock()
        client.mqtt.publish_command_message = MagicMock()
        return client

    def test_unsupported_command_ignored(self, mock_client):
        """Unsupported command is ignored (no publish, no error)."""
        main_module.handle_command(
            mock_client,
            "twin-123",
            "unknown_command",
            {},
        )
        mock_client.mqtt.publish_command_message.assert_not_called()

    def test_empty_command_ignored(self, mock_client):
        """Empty or invalid command is ignored."""
        main_module.handle_command(mock_client, "twin-123", "", {})
        mock_client.mqtt.publish_command_message.assert_not_called()

    def test_so101_prefix_stripped(self, mock_client):
        """Command with so101- prefix is normalized (e.g. so101-remoteoperate -> remoteoperate)."""
        with patch.object(
            main_module, "start_remoteoperate", MagicMock()
        ) as mock_start:
            with patch.object(main_module, "_stop_current_operation"):
                main_module.handle_command(
                    mock_client,
                    "twin-123",
                    "so101-remoteoperate",
                    {},
                )
        mock_start.assert_called_once_with(mock_client, "twin-123")

    def test_controller_changed_dispatches(self, mock_client):
        """controller-changed command calls _handle_controller_changed."""
        with patch.object(
            main_module, "_handle_controller_changed", MagicMock()
        ) as mock_handler:
            main_module.handle_command(
                mock_client,
                "twin-123",
                "controller-changed",
                {"controller_type": "localop"},
            )
        mock_handler.assert_called_once_with(
            mock_client, "twin-123", {"controller_type": "localop"}
        )
