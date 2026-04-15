"""Image handling utilities for HTML conversion.

This module provides functionality to convert image files to HTML image tags
using base64 encoding for inline display.
"""

from base64 import b64encode
from typing import Literal

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ImgHandler(metaclass=TypeChecker):
    """Handler for converting images to HTML format.

    This class provides methods to convert image files to HTML image tags
    using base64 encoding for inline display in web applications.
    """

    def img_to_html(
        self, 
        complete_path: str, format: Literal['jpeg', 'png', 'gif'] = "jpeg"
    ) -> str:
        """Convert an image file to an HTML image tag with base64 encoding.

        Parameters
        ----------
        complete_path : str
            Full path to the image file to be converted
        format : Literal['jpeg', 'png', 'gif']
            Image format for the output (default: 'jpeg')

        Returns
        -------
        str
            HTML image tag with base64 encoded image data

        Raises
        ------
        FileNotFoundError
            If the image file cannot be found at the specified path
        """
        self._validate_format(format)

        try:
            with open(complete_path, "rb") as image_file:
                image_data = image_file.read()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Image file not found at path: {complete_path}") from e

        data_uri = b64encode(image_data).decode("utf-8")
        img_tag = f'<img src="data:image/{format};base64,{data_uri}">'
        return img_tag

    def _validate_format(self, format: str) -> None:
        """Validate that the image format is supported.

        Parameters
        ----------
        format : str
            Image format to validate

        Raises
        ------
        ValueError
            If the format is not one of the supported options
        """
        supported_formats = {"jpeg", "png", "gif"}
        if format.lower() not in supported_formats:
            raise ValueError(
                f"Unsupported image format: {format}. Supported formats are: {supported_formats}"
            )