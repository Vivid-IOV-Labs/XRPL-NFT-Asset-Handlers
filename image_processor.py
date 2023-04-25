from io import BytesIO
from PIL import Image, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True


def process_image(content: bytes):
    img = Image.open(BytesIO(content))
    width, height = img.size
    aspect_ratio = width / height
    new_height = 200
    new_width = int(aspect_ratio * new_height)
    new_image = img.resize((new_width, new_height))
    return img, new_image
