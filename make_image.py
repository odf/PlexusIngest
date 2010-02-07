"""
Functions to convert a two-dimensional numpy array into image data
ready to be written to file. The main entry point is make_image().
"""

from cStringIO import StringIO

from numpy import *
from PIL import Image


# -- possible values for the conversion mode
GRAYSCALE         = 'grayscale'
BLACK_AND_WHITE   = 'black_and_white'
COLOR_CODED       = 'color_coded'
COLOR_CODED_FIXED = 'color_coded_fixed'


def convert_grayscale(data, mask, lo, hi):
    """
    Converts two-dimensional grayscale data into an image object. Input
    should be of type 'uint16'.
    
    The numpy array <data> contains the (masked) raw data. A
    second array <mask> of equal dimensions is set to 1 for
    portions of the data that were suppressed. At any point at most
    one of these arrays should be non-zero.
    
    The numbers <lo> and <hi> mark the range of relevant data
    values. For tomo images, contrast is stretched linearly so that
    lo becomes black and hi becomes white.
    
    The results is an 8-bit grayscale PIL.Image object.
    """
    
    # -- stretch contrast linearly between lo and hi
    f = float(0xffff) / (hi - lo) / 256
    output = minimum((maximum(data, lo) - lo) * f, 255)
    # -- convert to unsigned byte and compose with mask
    output = (output.astype(uint8) | (mask.astype(uint8) * 80))
    # -- convert to image
    return Image.fromarray(output, 'L')


def convert_black_and_white(data, mask):
    """
    Converts two-dimensional data into a black-and-white image.
    Input can be of any integral type. See 'convert_grayscale' for a
    description of the <data> and <mask> parameters.
    
    The results is an 8-bit grayscale PIL.Image object.
    """
    output = (mask * 80 + where(data > 0, 0xff, 0)).astype(uint8)
    return Image.fromarray(output, 'L')


def convert_color_coded(data, mask, use_fixed = False):
    """
    Converts two-dimensional data into an image object by applying
    a color-coding scheme. Input can be of any integral type, but only
    the 16 least significant bits are used. See 'convert_grayscale' for
    a description of the <data> and <mask> parameters.
    
    Data values are converted into colors by means of a bit
    shuffling scheme in order to increase the visual contrast
    between phases. If <use_fixed> is true, the first 10 non-zero
    colors are taken from a fixed palette.
    
    The results is an RGBA-encoded PIL.Image object.
    """
    
    # -- fill output array with opaque black pixels
    output = zeros(data.shape, uint32) + 0xff000000
    # -- apply mask
    output[mask != 0] |= 0x505050
    
    if (use_fixed):
        # -- map low phase values to colors: green, red, blue, etc.
        colormap = [ 0,
                     0x00ff00, 0x0000ff, 0xff0000, 0x00ffff, 0xffff00,
                     0x007f00, 0x00007f, 0x7f0000, 0x007f7f, 0x7f7f00 ]
        for i in range(1, len(colormap)):
            output[data == i] |= colormap[i]
        data = where(data < len(colormap), 0, data)

    # -- translation map to turn label bits into RGB bits
    bmap = [ 7, 15, 23, 6, 14, 22, 5, 13, 21, 4, 12, 20, 3, 11, 19 ]
    # -- shuffle input bits according to bmap
    for i in range(15):
        output |= ((data >> i) & 1) << bmap[i]

    # -- convert to image
    return Image.fromarray(output, 'RGBA')


def make_image(a, lo, hi, mask_val, mode):
    """
    Turns the 2d numpy array <a> into an image, with the string
    <mode> indicating how to convert scalar values into colors. The
    numbers <lo> and <hi> mark the range of relevant data values.
    Entries of value <mask_val> in the input array are interpreted as
    'masked out'.
    
    The image data is encoded in '.png' format and returned as a
    binary string.
    """
    
    # -- separate mask and data
    data = where(a == mask_val, 0, a)
    mask = where(a == mask_val, 1, 0)
    
    # -- convert into PIL.Image object depending on format
    if mode == GRAYSCALE:
        image = convert_grayscale(data, mask, lo, hi)
    elif mode == BLACK_AND_WHITE:
        image = convert_black_and_white(data, mask)
    elif mode == COLOR_CODED:
        image = convert_color_coded(data, mask, False)
    elif mode == COLOR_CODED_FIXED:
        image = convert_color_coded(data, mask, True)
    else:
        raise "unknown mode: '%s'" % mode

    # -- extract the image data in .png format into a binary string
    output = StringIO()
    image.save(output, "PNG")
    content = output.getvalue();
    output.close()
    
    # -- return the data string encoding the image
    return content

def make_dummy(text, width = 256, height = 256):
    from PIL import ImageDraw

    image = Image.new("RGB", (width, height), 'gray')
    draw = ImageDraw.Draw(image)
    draw.text((64, 64), text, fill = 'black')

    # -- extract the image data in .png format into a binary string
    output = StringIO()
    image.save(output, "PNG")
    content = output.getvalue();
    output.close()
    
    # -- return the data string encoding the image
    return content
