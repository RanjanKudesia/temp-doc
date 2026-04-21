"""Shared edit services."""

from . import json_patch_service
from .json_patch_service import edit_extracted_json

__all__ = ["json_patch_service", "edit_extracted_json"]
