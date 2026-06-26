"""
Tests for src/notifications.py — Discord delivery. requests is mocked, so no
network. Focus: the monthly briefing goes out as a SINGLE message with the full
report attached as a file (not split into many 2000-char messages).
"""

from unittest.mock import MagicMock, patch

from src.notifications import DiscordNotifier


def _notifier():
    return DiscordNotifier(webhook_url="https://discord.com/api/webhooks/test")


class TestDiscordBriefing:
    def test_sends_single_file_attachment(self):
        long_report = "## SECTION\n" + ("data line\n" * 3000)  # well over 2000 chars
        with patch("src.notifications.requests.post") as post:
            post.return_value = MagicMock(status_code=204)
            ok = _notifier().send_briefing(long_report)

        assert ok is True
        assert post.call_count == 1  # ONE message, not chunked
        kwargs = post.call_args.kwargs
        assert "files" in kwargs and "data" in kwargs  # multipart upload
        fname, payload, mime = kwargs["files"]["file"]
        assert fname.endswith(".md")
        assert payload == long_report.encode("utf-8")

    def test_falls_back_to_chunks_if_upload_fails(self):
        report = "line\n" * 3000
        with patch("src.notifications.requests.post") as post:
            # First call (file upload) fails; subsequent (chunked embeds) succeed.
            post.side_effect = [MagicMock(status_code=400)] + [MagicMock(status_code=204)] * 20
            ok = _notifier().send_briefing(report)

        assert ok is True
        assert post.call_count > 1  # fell back to multiple chunk messages

    def test_no_op_when_not_configured(self):
        n = DiscordNotifier(webhook_url=None)
        with patch("src.notifications.requests.post") as post:
            assert n.send_briefing("x") is False
            post.assert_not_called()
