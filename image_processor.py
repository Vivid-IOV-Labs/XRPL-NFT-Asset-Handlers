from io import BytesIO
from PIL import Image, ImageFile
from typing import Optional

ImageFile.LOAD_TRUNCATED_IMAGES = True


def process_image(content: bytes):
    img = Image.open(BytesIO(content))
    width, height = img.size
    aspect_ratio = width / height
    new_height = 200
    new_width = int(aspect_ratio * new_height)
    new_image = img.resize((new_width, new_height))
    return img, new_image


def resize_image(content: bytes, req_height: Optional[int], req_width: Optional[int]) -> BytesIO:
    img = Image.open(BytesIO(content))  # noqa
    width, height = img.size
    aspect_ratio = width / height

    if req_height:
        req_width = req_height * aspect_ratio
    elif req_width:
        req_height = int(req_width * 1 / aspect_ratio)
    else:
        req_width, req_height = width, height
    new_image = img.resize((req_width, req_height))
    output_buffer = BytesIO()
    new_image.save(output_buffer, format="JPEG")
    return output_buffer
