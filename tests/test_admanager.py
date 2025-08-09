import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.append(str(Path(__file__).resolve().parents[1]))
from GoogleCloudPlatformAPI.AdManager import AudienceService


class TestAdManager(unittest.TestCase):
    @patch("GoogleCloudPlatformAPI.AdManager.GamClient")
    def test_audience_service_list(self, mock_gam_client):
        """Test the list method of the AudienceService."""
        # Arrange
        mock_service = MagicMock()
        mock_gam_instance = MagicMock()
        mock_gam_instance.get_service.return_value = mock_service
        mock_gam_client.return_value = mock_gam_instance

        # Simulate two pages of results
        mock_service.getAudienceSegmentsByStatement.side_effect = [
            {"results": [{"id": 1, "name": "Segment 1"}, {"id": 2, "name": "Segment 2"}]},
            {"results": [{"id": 3, "name": "Segment 3"}]},
            {"results": []},  # Empty page to terminate the loop
        ]

        # Act
        audience_service = AudienceService()
        results = audience_service.list()

        # Assert
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["name"], "Segment 1")
        self.assertEqual(results[2]["name"], "Segment 3")
        self.assertEqual(mock_service.getAudienceSegmentsByStatement.call_count, 3)


if __name__ == "__main__":
    unittest.main()
