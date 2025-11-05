"""
Format and extension validation functions.

This module provides functions to validate file formats and extensions
against supported types for xarray and rasterio processing.
"""

from typing import Optional
from .constants import SUPPORTED_FORMATS, SUPPORTED_EXTENSIONS


def is_supported(
    value: str,
    check_formats: bool = True,
    check_extensions: bool = True
) -> bool:
    """
    Check if a value is a supported format or extension.

    Args:
        value: The format or extension string to check
        check_formats: Whether to check against supported formats
        check_extensions: Whether to check against supported extensions

    Returns:
        True if the value is supported, False otherwise
    """
    value_lower = value.lower()

    if check_formats:
        if value_lower in [fmt.lower() for fmt in SUPPORTED_FORMATS]:
            return True

    if check_extensions:
        if value_lower in [ext.lower() for ext in SUPPORTED_EXTENSIONS]:
            return True

    return False


def is_supported_format(file_format: str) -> bool:
    """
    Check if the file format is supported for xarray opening.

    Args:
        file_format: File format string

    Returns:
        True if format is supported, False otherwise
    """
    return is_supported(file_format, check_formats=True, check_extensions=True)


def is_supported_extension(file_ext: str) -> bool:
    """
    Check if the file extension is supported.

    Args:
        file_ext: File extension string

    Returns:
        True if extension is supported, False otherwise
    """
    return is_supported(file_ext, check_formats=False, check_extensions=True)
