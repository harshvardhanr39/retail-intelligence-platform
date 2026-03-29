import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

class TestGetWatermark:

    def test_returns_default_when_no_watermark_exists(self):
        """First run should return 90 days ago."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # no watermark in DB

        with patch("psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn

            from pipeline.utils.watermark import get_watermark
            result = get_watermark("orders")

        expected = datetime.utcnow() - timedelta(days=90)
        diff = abs((result - expected).total_seconds())
        assert diff < 10  # within 10 seconds of expected

    def test_returns_stored_watermark(self):
        """Should return the stored timestamp from DB."""
        stored_time = datetime(2024, 1, 15, 12, 0, 0)
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (stored_time,)

        with patch("psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn

            from pipeline.utils.watermark import get_watermark
            result = get_watermark("orders")

        assert result == stored_time


class TestUpdateWatermark:

    def test_update_watermark_executes_upsert(self):
        """Should execute INSERT ... ON CONFLICT DO UPDATE."""
        mock_cursor = MagicMock()

        with patch("psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn

            from pipeline.utils.watermark import update_watermark
            new_wm = datetime(2024, 1, 20, 3, 0, 0)
            update_watermark("orders", new_wm)

        # Verify execute was called
        assert mock_cursor.execute.called
        call_args = mock_cursor.execute.call_args[0]
        assert "INSERT" in call_args[0]
        assert "ON CONFLICT" in call_args[0]
